import requests
import pytz
from dotenv import load_dotenv
import os
from supabase import create_client
from datetime import datetime

# connect to database and initialize client
load_dotenv()
supabase_url=os.getenv("SUPABASE_URL")
supabase_key=os.getenv("SUPABASE_KEY")
if not supabase_url or not supabase_key:
    print("error, please check .env file")
else:
    supabase=create_client(supabase_url, supabase_key)
    print("successfully connected to supabase")

# test insert data
def test_insert():
    test={'sno':'000000000',
          'sna':'test_station',
          'sarea':'test_area',
          'latitude':99.99,
          'longitude':-99.99,
          'ar':'test_address',
          'quantity':999}
    response=supabase.table('station_info').insert(test).execute()
    print('success' if response.data else 'failed')

# fetch data from api and return json data
def ubike_fetch():
    ubike_url="https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json"
    try:
        response=requests.get(ubike_url)
        data=response.json()
        print('successfully fetch data')
    except Exception as e:
        print(f'error:\n{e}')
        data=None
    return data

# insert station info data into database
def station_info_insert():
    data=ubike_fetch()
    if not data:
        print('no data to insert')
        return None
    stations=[]
    for item in data:
        if item['sarea']=='大安區':
            station={
                'sno':item['sno'],
                'sna':item['sna'],
                'sarea':item['sarea'],
                'latitude':item['latitude'],
                'longitude':item['longitude'],
                'ar':item['ar'],
                'quantity':item['Quantity']
            }
            stations.append(station)
    response=supabase.table('station_info').insert(stations).execute()
    print('successfully insert data' if response.data else 'failed to insert data')

# insert bike info data into database
def bike_info_insert():
    tw_tz=pytz.timezone('Asia/Taipei')
    data=ubike_fetch()
    if not data:
        print('no data to insert')
        return None
    bikes=[]
    for item in data:
        if item['sarea']=='大安區':
            mday_raw=datetime.strptime(item['mday'], '%Y-%m-%d %H:%M:%S')
            mday_tw=tw_tz.localize(mday_raw).isoformat()
            bike={
                'sno':item['sno'],
                'mday':mday_tw,
                'available_return_bikes':item['available_return_bikes'],
                'act':item['act'],
                'fetch_time':datetime.now(pytz).isoformat()
            }
            bikes.append(bike)
    response=supabase.table('bike_info').insert(bikes).execute()
    print('successfully insert data' if response.data else 'failed to insert data')

if __name__ == "__main__":
    # station_info_insert()
    bike_info_insert()


