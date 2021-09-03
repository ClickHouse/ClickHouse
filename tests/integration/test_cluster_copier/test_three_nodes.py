import os
import sys
import time
import logging
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

import docker

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(CURRENT_TEST_DIR))

cluster = ClickHouseCluster(__file__, name='copier_test_three_nodes')

@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:

        for name in ["first", "second", "third"]:
            cluster.add_instance(name,
                main_configs=["configs_three_nodes/conf.d/clusters.xml", "configs_three_nodes/conf.d/ddl.xml"], user_configs=["configs_three_nodes/users.xml"],
                with_zookeeper=True)

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

class Task:
    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = '/clickhouse-copier/task'
        self.container_task_file = "/task_taxi_data.xml"

        for instance_name, _ in cluster.instances.items():
            instance = cluster.instances[instance_name]
            instance.copy_file_to_container(os.path.join(CURRENT_TEST_DIR, './task_taxi_data.xml'), self.container_task_file)
            logging.debug(f"Copied task file to container of '{instance_name}' instance. Path {self.container_task_file}")


    def start(self):
        for name in ["first", "second", "third"]:
            node = cluster.instances[name]
            node.query("DROP DATABASE IF EXISTS dailyhistory SYNC;")
            node.query("DROP DATABASE IF EXISTS monthlyhistory SYNC;")

        first = cluster.instances['first']

        # daily partition database
        first.query("CREATE DATABASE IF NOT EXISTS dailyhistory on cluster events;")
        first.query("""CREATE TABLE dailyhistory.yellow_tripdata_staging ON CLUSTER events
        (
            id UUID DEFAULT generateUUIDv4(),
            vendor_id String,
            tpep_pickup_datetime DateTime('UTC'),
            tpep_dropoff_datetime DateTime('UTC'),
            passenger_count Nullable(Float64),
            trip_distance String,
            pickup_longitude Float64,
            pickup_latitude Float64,
            rate_code_id String,
            store_and_fwd_flag String,
            dropoff_longitude Float64,
            dropoff_latitude Float64,
            payment_type String,
            fare_amount String,
            extra String,
            mta_tax String,
            tip_amount String,
            tolls_amount String,
            improvement_surcharge String,
            total_amount String,
            pickup_location_id String,
            dropoff_location_id String,
            congestion_surcharge String,
            junk1 String,  junk2 String
        )
        Engine = ReplacingMergeTree()
        PRIMARY KEY (tpep_pickup_datetime, id)
        ORDER BY (tpep_pickup_datetime, id)
        PARTITION BY (toYYYYMMDD(tpep_pickup_datetime))""")

        first.query("""CREATE TABLE dailyhistory.yellow_tripdata
            ON CLUSTER events
            AS dailyhistory.yellow_tripdata_staging
            ENGINE = Distributed('events', 'dailyhistory', yellow_tripdata_staging, sipHash64(id) % 3);""")

        first.query("""INSERT INTO dailyhistory.yellow_tripdata
            SELECT * FROM generateRandom(
                'id UUID DEFAULT generateUUIDv4(),
                vendor_id String,
                tpep_pickup_datetime DateTime(\\'UTC\\'),
                tpep_dropoff_datetime DateTime(\\'UTC\\'),
                passenger_count Nullable(Float64),
                trip_distance String,
                pickup_longitude Float64,
                pickup_latitude Float64,
                rate_code_id String,
                store_and_fwd_flag String,
                dropoff_longitude Float64,
                dropoff_latitude Float64,
                payment_type String,
                fare_amount String,
                extra String,
                mta_tax String,
                tip_amount String,
                tolls_amount String,
                improvement_surcharge String,
                total_amount String,
                pickup_location_id String,
                dropoff_location_id String,
                congestion_surcharge String,
                junk1 String,
                junk2 String',
            1, 10, 2) LIMIT 50;""")

        # monthly partition database
        first.query("create database IF NOT EXISTS monthlyhistory on cluster events;")
        first.query("""CREATE TABLE monthlyhistory.yellow_tripdata_staging ON CLUSTER events
        (
            id UUID DEFAULT generateUUIDv4(),
            vendor_id String,
            tpep_pickup_datetime DateTime('UTC'),
            tpep_dropoff_datetime DateTime('UTC'),
            passenger_count Nullable(Float64),
            trip_distance String,
            pickup_longitude Float64,
            pickup_latitude Float64,
            rate_code_id String,
            store_and_fwd_flag String,
            dropoff_longitude Float64,
            dropoff_latitude Float64,
            payment_type String,
            fare_amount String,
            extra String,
            mta_tax String,
            tip_amount String,
            tolls_amount String,
            improvement_surcharge String,
            total_amount String,
            pickup_location_id String,
            dropoff_location_id String,
            congestion_surcharge String,
            junk1 String,
            junk2 String
        )
        Engine = ReplacingMergeTree()
        PRIMARY KEY (tpep_pickup_datetime, id)
        ORDER BY (tpep_pickup_datetime, id)
        PARTITION BY (pickup_location_id, toYYYYMM(tpep_pickup_datetime))""")

        first.query("""CREATE TABLE monthlyhistory.yellow_tripdata
            ON CLUSTER events
            AS monthlyhistory.yellow_tripdata_staging
            ENGINE = Distributed('events', 'monthlyhistory', yellow_tripdata_staging, sipHash64(id) % 3);""")


    def check(self):
        first = cluster.instances["first"]
        a = TSV(first.query("SELECT count() from dailyhistory.yellow_tripdata"))
        b = TSV(first.query("SELECT count() from monthlyhistory.yellow_tripdata"))
        assert a == b, "Distributed tables"

        for instance_name, instance in cluster.instances.items():
            instance = cluster.instances[instance_name]
            a = instance.query("SELECT count() from dailyhistory.yellow_tripdata_staging")
            b = instance.query("SELECT count() from monthlyhistory.yellow_tripdata_staging")
            assert a == b, "MergeTree tables on each shard"

            a = TSV(instance.query("SELECT sipHash64(*) from dailyhistory.yellow_tripdata_staging ORDER BY id"))
            b = TSV(instance.query("SELECT sipHash64(*) from monthlyhistory.yellow_tripdata_staging ORDER BY id"))

            assert a == b, "Data on each shard"

        for name in ["first", "second", "third"]:
            node = cluster.instances[name]
            node.query("DROP DATABASE IF EXISTS dailyhistory SYNC;")
            node.query("DROP DATABASE IF EXISTS monthlyhistory SYNC;")



def execute_task(started_cluster, task, cmd_options):
    task.start()

    zk = started_cluster.get_kazoo_client('zoo1')
    logging.debug("Use ZooKeeper server: {}:{}".format(zk.hosts[0][0], zk.hosts[0][1]))

    # Run cluster-copier processes on each node
    docker_api = started_cluster.docker_client.api
    copiers_exec_ids = []

    cmd = ['/usr/bin/clickhouse', 'copier',
           '--config', '/etc/clickhouse-server/config-copier.xml',
           '--task-path', task.zk_task_path,
           '--task-file', task.container_task_file,
           '--task-upload-force', 'true',
           '--base-dir', '/var/log/clickhouse-server/copier']
    cmd += cmd_options

    logging.debug(f"execute_task cmd: {cmd}")

    for instance_name in started_cluster.instances.keys():
        instance = started_cluster.instances[instance_name]
        container = instance.get_docker_handle()
        instance.copy_file_to_container(os.path.join(CURRENT_TEST_DIR, "configs_three_nodes/config-copier.xml"), "/etc/clickhouse-server/config-copier.xml")
        logging.info("Copied copier config to {}".format(instance.name))
        exec_id = docker_api.exec_create(container.id, cmd, stderr=True)
        output = docker_api.exec_start(exec_id).decode('utf8')
        logging.info(output)
        copiers_exec_ids.append(exec_id)
        logging.info("Copier for {} ({}) has started".format(instance.name, instance.ip_address))

    # time.sleep(1000)

    # Wait for copiers stopping and check their return codes
    for exec_id, instance in zip(copiers_exec_ids, iter(started_cluster.instances.values())):
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
@pytest.mark.timeout(600)
def test(started_cluster):
    execute_task(started_cluster, Task(started_cluster), [])
