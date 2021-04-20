import os
import sys
import logging
import subprocess

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(CURRENT_TEST_DIR))


FIRST_DATA = [
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-12.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-11.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-10.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-09.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-08.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-07.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-06.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-05.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-04.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-03.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-02.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-12.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-11.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-10.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-09.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-08.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-07.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-06.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-05.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-04.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-03.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-02.csv",
"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv",
]

def prepare_data():
    first_path = os.path.join(CURRENT_TEST_DIR, "data/first.csv")
    # if os.path.exists(first_path):
    #     return

    # Create file
    with open(first_path, 'w') as _:
        pass

    print("{} created".format(first_path))
    
    for url in FIRST_DATA[:1]:
        print(url)
        subprocess.call("wget -O - {} 2> /dev/null | tail -n +3 | head -n 100 >> {}".format(url, first_path), shell = True)
        logging.info("Successfully downloaded data from {}".format(url))


if __name__ == "__main__":
    prepare_data()