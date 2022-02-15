import boto3
import requests

def lambda_handler(event, context):
    """Grab the weekly data from forexfactory and load into DynamoDB"""

    r = requests.get('https://nfs.faireconomy.media/ff_calendar_thisweek.json')
    
    if r.status_code != 200:
        print(f'Error fetching calendar data. Status code: {r.status_code}')
        return

    data = r.json()

    if not data:
        print('No data was returned from calendar request.')
        return
    
    # there's no unique values in any of the keys in this data so I need to make a custom key
    for item in data:
        custom_key = f"{item['country']}_{item['title']} {item['date']}"
        item['custom_key'] = custom_key

    # load data
    dynamodb = boto3.resource('dynamodb',region_name='us-east-2') 
    table = dynamodb.Table('ff_calendar_raw')
    for item in data:
        table.put_item(Item=item)

    # log how many items were 
    print(f'Loaded {len(data)} calendar items to ff_calendar_raw.')

