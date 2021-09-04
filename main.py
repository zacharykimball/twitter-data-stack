import tweepy
from google.cloud import bigquery
import json
import io
import os
import config
import time

def pull_tweets(event=None, context=None):
    if context is not None:
        print("Pull tweets was triggered by messageId {} published at {} to {}.".format(context.event_id, context.timestamp, context.resource["name"]))

    # Authorize with Twitter API v1.1
    auth = tweepy.OAuthHandler(config.twitter_consumer_key, config.twitter_consumer_secret)
    auth.set_access_token(config.twitter_access_token, config.twitter_access_token_secret)
    api = tweepy.API(auth)
    print('Successfully authorized Twitter v1.1.')
    
    # Set up BigQuery client
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.gcp_key_file
    client = bigquery.Client()
    print('Successfully authorized BigQuery.')
    
    # Note: We first pull a list of users associated with the list, then iterate over the users.
    # This is done because the API for pulling tweets directly from a list (i.e., api.list_timeline) returns more limited data.

    # Pull users associated with this list
    # Setting 1,000 members as the list limit
    list_members = api.list_members(list_id=config.twitter_list_id, count=1000)

    # Set up list to hold the data we care about into a clean JSON-formatted list
    tweets = list()

    # Cycle through individual user timelines to get full tweet history (using v2 API)
    for user in list_members:
        print('Pulling tweets for user ' + user._json['screen_name'] + '.')

        # If BigQuery already has data, pull the latest tweet ID (below we set the API call to get only newer tweets)
        # We do this on a user-by-user basis every time because if a new user is added to the list, we want to pull their history
        qry = "SELECT MAX(id) FROM `" + config.bq_dataset_id + "`.`" + config.bq_table_id + "` WHERE user.id = " + str(user._json['id'])
        query_job = client.query(qry)
        
        if query_job.errors is None:
            # Query was successful and we can pull the most recent tweet ID
            # Note: if the user is missing, this will resolve to None, which is correct
            user_latest_id = next(query_job.result())[0]
        elif query_job.errors[0]['reason'] == 'notFound':
            # Query broke because table does not exist; we continue on with no restriction
            user_latest_id = None
        else:
            # Something else went wrong; terminate to avoid creating duplicate data
            print('Unexpected query result for max ID.')
            raise RuntimeError(query_job.errors[0]['message'])
    
        # Pull tweets from user ID (200 is the limit for this API)
        tweet_api_data = tweepy.Cursor(api.user_timeline, user_id=user._json['id'], include_rts=False, count=100, since_id=user_latest_id, tweet_mode='extended').items(500)
        print('Successfully pulled Twitter data.')
    
        for t in tweet_api_data:
            payload = t._json
            tweets.append(
                    json.dumps(
                        {
                            'id': payload['id'],
                            'created_at': payload['created_at'],
                            'user': {
                                'id': payload['user']['id'],
                                'name': payload['user']['name'],
                                'screen_name': payload['user']['screen_name'],
                                'location': payload['user']['location'],
                                'description': payload['user']['description'],
                                'protected': payload['user']['protected'],
                                'verified': payload['user']['verified'],
                                'url': payload['user']['url'],
                                'followers_count': payload['user']['followers_count'],
                                'friends_count': payload['user']['friends_count'],
                                'listed_count': payload['user']['listed_count'],
                                'favourites_count': payload['user']['favourites_count'],
                                'statuses_count': payload['user']['statuses_count'],
                                'created_at': payload['user']['created_at']
                                },
                            'full_text': payload['full_text'],
                            'truncated': payload['truncated'],
                            'source': payload['source'],
                            'entities': {
                                'hashtags': {'text': hashtag['text'] for hashtag in payload['entities']['hashtags']},
                                'media': [{'id': media['id'], 'type': media['type'], 'media_url_https': media['media_url_https']} for media in payload['entities']['media']]
                                    if 'media' in payload['entities'] else None,
                                'urls': {'expanded_url': url['expanded_url'] for url in payload['entities']['urls']},
                                'user_mentions': [{'id': mention['id'], 'screen_name': mention['screen_name']} for mention in payload['entities']['user_mentions']]
                                },
                            'is_quote_status': payload['is_quote_status'],
                            'quoted_status_id': 
                                # Note: we need to test BOTH is_quote_status and quoted_status_id because a quote tweet of a deleted tweet
                                # will show is_quote_status = True but be missing the 'quoted_status_id' key
                                payload['quoted_status_id'] if payload['is_quote_status'] and 'quoted_status_id' in payload else None,
                            'in_reply_to_status_id': payload['in_reply_to_status_id'],
                            'in_reply_to_user_id': payload['in_reply_to_user_id'],
                            'loaded_at': time.time()
                        }
                    )
                )
    
    # Check if we found new data (and if so, add to BigQuery)
    if len(tweets) > 0:
        print('Successfully extracted data for ' + str(len(tweets)) + ' tweet(s).')
    
        # Set config for BigQuery job
        table_ref = client.dataset(config.bq_dataset_id).table(config.bq_table_id)
        bq_job_config = bigquery.LoadJobConfig(
            source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema = [
                    bigquery.SchemaField('id', 'INT64', 'REQUIRED'),
                    bigquery.SchemaField('created_at', 'STRING', 'REQUIRED'),
                    bigquery.SchemaField('user', 'RECORD', 'REQUIRED',
                        fields = [
                            bigquery.SchemaField('id', 'INT64', 'REQUIRED'),
                            bigquery.SchemaField('name', 'STRING', 'REQUIRED'),
                            bigquery.SchemaField('screen_name', 'STRING', 'REQUIRED'),
                            bigquery.SchemaField('location', 'STRING', 'NULLABLE'),
                            bigquery.SchemaField('description', 'STRING', 'NULLABLE'),
                            bigquery.SchemaField('protected', 'BOOLEAN', 'REQUIRED'),
                            bigquery.SchemaField('verified', 'BOOLEAN', 'REQUIRED'),
                            bigquery.SchemaField('url', 'STRING', 'NULLABLE'),
                            bigquery.SchemaField('followers_count', 'INTEGER', 'REQUIRED'),
                            bigquery.SchemaField('friends_count', 'INTEGER', 'REQUIRED'),
                            bigquery.SchemaField('listed_count', 'INTEGER', 'REQUIRED'),
                            bigquery.SchemaField('favourites_count', 'INTEGER', 'REQUIRED'),
                            bigquery.SchemaField('statuses_count', 'INTEGER', 'REQUIRED'),
                            bigquery.SchemaField('created_at', 'STRING', 'REQUIRED')
                            ]
                        ),
                    bigquery.SchemaField('full_text', 'STRING', 'REQUIRED'),
                    bigquery.SchemaField('truncated', 'BOOLEAN', 'REQUIRED'),
                    bigquery.SchemaField('source', 'STRING', 'NULLABLE'),
                    bigquery.SchemaField('entities', 'RECORD', 'NULLABLE',
                        fields = [
                            bigquery.SchemaField('hashtags', 'RECORD', 'REPEATED',
                                fields = [
                                    bigquery.SchemaField('text', 'STRING', 'NULLABLE')
                                    ]
                                ),
                            bigquery.SchemaField('media', 'RECORD', 'REPEATED',
                                fields = [
                                    bigquery.SchemaField('id', 'INT64', 'NULLABLE'),
                                    bigquery.SchemaField('type', 'STRING', 'NULLABLE'),
                                    bigquery.SchemaField('media_url_https', 'STRING', 'NULLABLE')
                                    ]
                                ),
                            bigquery.SchemaField('urls', 'RECORD', 'REPEATED',
                                fields = [
                                    bigquery.SchemaField('expanded_url', 'STRING', 'NULLABLE')
                                    ]
                                ),
                            bigquery.SchemaField('user_mentions', 'RECORD', 'REPEATED',
                                fields = [
                                    bigquery.SchemaField('id', 'INT64', 'NULLABLE'),
                                    bigquery.SchemaField('screen_name', 'STRING', 'NULLABLE')
                                    ]
                                )
                            ]
                        ),
                    bigquery.SchemaField('is_quote_status', 'BOOLEAN', 'REQUIRED'),
                    bigquery.SchemaField('quoted_status_id', 'INT64', 'NULLABLE'),
                    bigquery.SchemaField('in_reply_to_status_id', 'INT64', 'NULLABLE'),
                    bigquery.SchemaField('in_reply_to_user_id', 'INT64', 'NULLABLE'),
                    bigquery.SchemaField('loaded_at', 'TIMESTAMP', 'REQUIRED')
                ],
            autodetect = False
            )
        
        # (1) Per https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_json
        #   better to convert JSON to file-like object and pass to load_table_from_file, rather than use load_table_from_json
        # (2) Joining entries with a newline to be compliant with BQ JSON syntax
        # (3) N.B. If table does not exist, it will be created
        data_as_file = io.StringIO('\n'.join(tweets))
        load_job = client.load_table_from_file(
           data_as_file,
           table_ref,
           job_config = bq_job_config)
        load_job.result()
    
        if load_job.errors is None:
            print('Successfully loaded data to BigQuery.')
        else:
            print('Error loading data to BigQuery.')
    else:
        print('No new Twitter data found.')

def action_tweets(event=None, context=None):
    if context is not None:
        print("Action tweets was triggered by messageId {} published at {} to {}.".format(context.event_id, context.timestamp, context.resource["name"]))
    
    # Set up BigQuery client
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.gcp_key_file
    client = bigquery.Client()
    print('Successfully authorized BigQuery.')
    
    # If BigQuery already has data, pull the latest tweet ID (below we set the API call to get only newer tweets)
    # N.B. obviously this is not injection-safe, but if someone has access to your config file, you have bigger problems...
    qry = "SELECT * FROM `" + config.bq_dataset_analysis_id + "`.`" + config.bq_table_actions_id + "` WHERE actioned = false"
    query_job = client.query(qry)
    
    if query_job.errors is None:
        # Query was successful and we can proceed with actions
        actioned_user_ids = list()

        # Authorize with Twitter
        auth = tweepy.OAuthHandler(config.twitter_consumer_key, config.twitter_consumer_secret)
        auth.set_access_token(config.twitter_access_token, config.twitter_access_token_secret)
        api = tweepy.API(auth)
        print('Successfully authorized Twitter.')

        # Set up BigQuery config for adding activity records
        table_ref = client.dataset(config.bq_dataset_id).table(config.bq_table_activity_id)
        bq_job_config = bigquery.LoadJobConfig(
            source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema = [
                    bigquery.SchemaField('action_status_id', 'INT64', 'REQUIRED'),
                    bigquery.SchemaField('actioned_user_id', 'INT64', 'REQUIRED'),
                    bigquery.SchemaField('payload', 'STRING', 'REQUIRED'),
                    bigquery.SchemaField('created_at', 'STRING', 'REQUIRED')
                ],
            autodetect = False
            )

        # Action each row in sequence
        for row in query_job.result():
            print('Tweeting at @' + row['latest_screen_name'])
            actioned_user_ids.append(row['user_id'])

            # Set up tweet based on action data
            tweet_text = '@{screen_name} r u okay? Haven''t heard from you in {days:.0f} days! ' \
                    'You normally tweet {baseline:.1f} times per day but only {recent:.1f} this past week...'.format(
                            screen_name = row['latest_screen_name'],
                            days = row['days_since_last_tweet'],
                            baseline = row['average_daily_activity_baseline'],
                            recent = row['average_daily_activity_recent']
                        )
            
            # Add reference to inspiration tweet
            tweet_text += ' https://twitter.com/clairebcarroll/status/1423628154065899525'

            # Send a tweet related to the action
            # THIS LINE IS COMMENTED OUT BY DEFAULT TO MAKE SURE THIS CODE DOES NOT TWEET ON YOUR BEHALF
            # UNCOMMENT THE FOLLOWING LINE TO ENABLE THIS SCRIPT TO TWEET 'U OK' MESSAGES
            #status = api.update_status(tweet_text)
            
            # TEMP -- create fake status so we're not actually tweeting
            status = {'id': 12345, 'created_at': 'Thu Aug 12 16:11:58 +0000 2021'}

            # Update BQ with a report about the relevant action
            update = json.dumps(
                    {
                        'action_status_id': status['id'],
                        'actioned_user_id': row['user_id'],
                        'payload': tweet_text,
                        'created_at': status['created_at']
                    }
                )

            print('Updating activity record for this tweet')
            data_as_file = io.StringIO(update)
            load_job = client.load_table_from_file(
               data_as_file,
               table_ref,
               job_config = bq_job_config)
            load_job.result()
        
            if load_job.errors is None:
                print('Successfully loaded action_status_id ' + str(status['id']) + ' to BigQuery.')
            else:
                print('Error loading action_status_id ' + str(status['id']) + ' to BigQuery.')

            time.sleep(1)

        if len(actioned_user_ids) > 0:
            # Set action status for all actioned users to make sure we don't accidentally tweet at them again (e.g., if dbt job fails to run)
            print('Setting action status for all actioned users')
            action_qry = 'UPDATE `' + config.bq_dataset_analysis_id + '`.`' + config.bq_table_actions_id + '` ' + \
                'SET actioned = true WHERE user_id IN (' + ','.join([str(id) for id in actioned_user_ids]) + ')'
            action_job = client.query(action_qry)
    
            if action_job.errors is None:
                print('Successfully set action status to BigQuery.')
            else:
                print('Error setting action status in BigQuery. Duplicate tweets could be possible!')
        else:
            print('No actions were found!')

    elif query_job.errors[0]['reason'] == 'notFound':
        # Query broke because table does not exist; our dbt models have not run yet
        print('Triggered action table does not exist; dbt hasn''t run yet or had an error.')
        # Exit gracefully
    else:
        # Something else went wrong; terminate to avoid creating duplicate data
        print('Unexpected query result for actions needed.')
        raise RuntimeError(query_job.errors[0]['message'])

if __name__ == '__main__':
    pull_tweets()
