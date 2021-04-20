import os
import sys
import time
import logging
import subprocess
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

import docker

logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(CURRENT_TEST_DIR))


COPYING_FAIL_PROBABILITY = 0.33
MOVING_FAIL_PROBABILITY = 0.1
cluster = None


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster = ClickHouseCluster(__file__)

        for name in ["first", "second", "third"]:
            cluster.add_instance(name,
                main_configs=["configs_taxi/conf.d/clusters.xml", "configs_taxi/conf.d/ddl.xml"], user_configs=["configs_taxi/users.xml"],
                with_zookeeper=True, external_data_path=os.path.join(CURRENT_TEST_DIR, "./data"))

        cluster.start()
        yield cluster

    finally:
        pass
        cluster.shutdown()


DATA_COMMANDS = [
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-12.csv 2> /dev/null | tail -n +3 |  time,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-11.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-10.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-09.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-08.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-07.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-06.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-05.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-04.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-03.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-02.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-01.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',

'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-12.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-11.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-10.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-09.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-08.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-07.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-06.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-05.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-04.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-03.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-02.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-01.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount) FORMAT CSV"',

'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-12.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-11.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-10.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-09.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-08.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-07.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-06.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-05.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-04.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-03.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-02.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2) FORMAT CSV"',
'wget -O - https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-01.csv 2> /dev/null | tail -n +3 | clickhouse-client --query="INSERT INTO dailyhistory.yellow_tripdata(vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2) FORMAT CSV"'
]


class Task:
    def __init__(self, cluster, use_sample_offset):
        self.cluster = cluster
        if use_sample_offset:
            self.zk_task_path = "/clickhouse-copier/task"
        else:
            self.zk_task_path = "/clickhouse-copier/task"
        self.copier_task_config = open(os.path.join(CURRENT_TEST_DIR, 'task_taxi_data.xml'), 'r').read()

    def start(self):
        instance = cluster.instances['first']

        # daily partition database
        instance.query("CREATE DATABASE dailyhistory on cluster events;")
        instance.query("""CREATE TABLE dailyhistory.yellow_tripdata_staging ON CLUSTER events
        (
            id UUID DEFAULT generateUUIDv4(),  vendor_id String,  tpep_pickup_datetime DateTime('UTC'),  tpep_dropoff_datetime DateTime('UTC'),  
            passenger_count Nullable(Float64),  trip_distance String,  pickup_longitude Float64,  pickup_latitude Float64,
            rate_code_id String,  store_and_fwd_flag String,  dropoff_longitude Float64,  dropoff_latitude Float64,
            payment_type String,  fare_amount String,  extra String,  mta_tax String,  tip_amount String,  tolls_amount String,
            improvement_surcharge String,  total_amount String,  pickup_location_id String,  dropoff_location_id String,  congestion_surcharge String,
            junk1 String,  junk2 String
        )
        Engine = ReplacingMergeTree() PRIMARY KEY (tpep_pickup_datetime, id) ORDER BY (tpep_pickup_datetime, id) PARTITION BY (toYYYYMMDD(tpep_pickup_datetime))""")
        instance.query("CREATE TABLE dailyhistory.yellow_tripdata ON CLUSTER events AS dailyhistory.yellow_tripdata_staging ENGINE = Distributed('events', 'dailyhistory', yellow_tripdata_staging, rand());")

        # monthly partition database
        instance.query("create database monthlyhistory on cluster events;")
        instance.query("""CREATE TABLE monthlyhistory.yellow_tripdata_staging ON CLUSTER events
        (
            id UUID DEFAULT generateUUIDv4(),  vendor_id String,  tpep_pickup_datetime DateTime('UTC'),  tpep_dropoff_datetime DateTime('UTC'),
            passenger_count Nullable(Float64),  trip_distance String,  pickup_longitude Float64,  pickup_latitude Float64,  rate_code_id String,
            store_and_fwd_flag String,  dropoff_longitude Float64,  dropoff_latitude Float64,  payment_type String,  fare_amount String,
            extra String,  mta_tax String,  tip_amount String,  tolls_amount String,  improvement_surcharge String,  total_amount String,
            pickup_location_id String,  dropoff_location_id String,  congestion_surcharge String,  junk1 String,  junk2 String
        ) 
        Engine = ReplacingMergeTree() PRIMARY KEY (tpep_pickup_datetime, id) ORDER BY (tpep_pickup_datetime, id) PARTITION BY (pickup_location_id, toYYYYMM(tpep_pickup_datetime))""")
        instance.query("CREATE TABLE monthlyhistory.yellow_tripdata ON CLUSTER events AS monthlyhistory.yellow_tripdata_staging ENGINE = Distributed('events', 'monthlyhistory', yellow_tripdata_staging, rand());")


        logging.info("Inserting in container")
        first_query = """INSERT INTO dailyhistory.yellow_tripdata(
            vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,
            rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,
            fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge) FORMAT CSV"""
        instance.exec_in_container(['bash', '-c', 'cat /usr/share/clickhouse-external-data/first.csv | /usr/bin/clickhouse client --query="{}"'.format(first_query)], privileged=True)
        logging.info("Insert completed")
    

    def check(self):
        pass


def execute_task(task, cmd_options):
    task.start()

    zk = cluster.get_kazoo_client('zoo1')
    print("Use ZooKeeper server: {}:{}".format(zk.hosts[0][0], zk.hosts[0][1]))

    # Run cluster-copier processes on each node
    docker_api = docker.from_env().api
    copiers_exec_ids = []

    cmd = ['/usr/bin/clickhouse', 'copier',
           '--config', '/etc/clickhouse-server/config-copier.xml',
           '--task-path', task.zk_task_path,
           '--task-file', os.path.join(CURRENT_TEST_DIR, 'task_taxi_data.py'),
           '--task-upload-force', ' ',
           '--base-dir', '/var/log/clickhouse-server/copier']
    cmd += cmd_options

    print(cmd)

    for instance_name, instance in cluster.instances.items():
        instance = cluster.instances[instance_name]
        container = instance.get_docker_handle()
        instance.copy_file_to_container(os.path.join(CURRENT_TEST_DIR, "configs_taxi/config-copier.xml"), "/etc/clickhouse-server/config-copier.xml")
        logging.info("Copied copier config to {}".format(instance.name))
        exec_id = docker_api.exec_create(container.id, cmd, stderr=True)
        output = docker_api.exec_start(exec_id).decode('utf8')
        logging.info(output)
        copiers_exec_ids.append(exec_id)
        logging.info("Copier for {} ({}) has started".format(instance.name, instance.ip_address))

    # time.sleep(1000)

    # Wait for copiers stopping and check their return codes
    for exec_id, instance in zip(copiers_exec_ids, iter(cluster.instances.values())):
        while True:
            res = docker_api.exec_inspect(exec_id)
            if not res['Running']:
                break
            time.sleep(1)

        assert res['ExitCode'] == 0, "Instance: {} ({}). Info: {}".format(instance.name, instance.ip_address, repr(res))

    try:
        task.check()
    finally:
        zk.delete(task.zk_task_path, recursive=True)


# Tests

def test1(started_cluster):
    execute_task(Task(started_cluster, False))

