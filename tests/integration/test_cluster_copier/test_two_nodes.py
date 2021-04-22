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

cluster = None


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster = ClickHouseCluster(__file__)

        for name in ["first", "second"]:
            cluster.add_instance(name,
                main_configs=["configs_two_nodes/conf.d/clusters.xml", "configs_two_nodes/conf.d/ddl.xml"], user_configs=["configs_two_nodes/users.xml"],
                with_zookeeper=True, external_data_path=os.path.join(CURRENT_TEST_DIR, "./data"))

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

# Will copy table from `first` node to `second`
class TaskWithDifferentSchema:
    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = '/clickhouse-copier/task_with_different_schema'
        self.container_task_file = "/task_with_different_schema.xml"

        for instance_name, _ in cluster.instances.items():
            instance = cluster.instances[instance_name]
            instance.copy_file_to_container(os.path.join(CURRENT_TEST_DIR, './task_with_different_schema.xml'), self.container_task_file)
            print("Copied task file to container of '{}' instance. Path {}".format(instance_name, self.container_task_file))

    def start(self):
        first = cluster.instances["first"]
        first.query("CREATE DATABASE db_different_schema;")
        first.query("""CREATE TABLE db_different_schema.source
        (
            Column1 String,
            Column2 UInt32,
            Column3 Date,
            Column4 DateTime,
            Column5 UInt16,
            Column6 String,
            Column7 String,
            Column8 String,
            Column9 String,
            Column10 String,
            Column11 String,
            Column12 Decimal(3, 1),
            Column13 DateTime,
            Column14 UInt16
        )
        ENGINE = MergeTree()
        PARTITION BY (toYYYYMMDD(Column3), Column3)
        PRIMARY KEY (Column1, Column2, Column3, Column4, Column6, Column7, Column8, Column9)
        ORDER BY (Column1, Column2, Column3, Column4, Column6, Column7, Column8, Column9)
        SETTINGS index_granularity = 8192""")

        first.query("""INSERT INTO db_different_schema.source SELECT * FROM generateRandom(
            'Column1 String, Column2 UInt32, Column3 Date, Column4 DateTime, Column5 UInt16,
            Column6 String, Column7 String, Column8 String, Column9 String, Column10 String,
            Column11 String, Column12 Decimal(3, 1), Column13 DateTime, Column14 UInt16', 1, 10, 2) LIMIT 50;""")

        second = cluster.instances["second"]
        second.query("CREATE DATABASE db_different_schema;")
        second.query("""CREATE TABLE db_different_schema.destination
        (
            Column1 LowCardinality(String) CODEC(LZ4),
            Column2 UInt32 CODEC(LZ4),
            Column3 Date CODEC(DoubleDelta, LZ4),
            Column4 DateTime CODEC(DoubleDelta, LZ4),
            Column5 UInt16 CODEC(LZ4),
            Column6 LowCardinality(String) CODEC(ZSTD),
            Column7 LowCardinality(String) CODEC(ZSTD),
            Column8 LowCardinality(String) CODEC(ZSTD),
            Column9 LowCardinality(String) CODEC(ZSTD),
            Column10 String CODEC(ZSTD(6)),
            Column11 LowCardinality(String) CODEC(LZ4),
            Column12 Decimal(3,1) CODEC(LZ4),
            Column13 DateTime CODEC(DoubleDelta, LZ4),
            Column14 UInt16 CODEC(LZ4)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(Column3)
        ORDER BY (Column9, Column1, Column2, Column3, Column4);""")
        
        print("Preparation completed")

    def check(self):
        first = cluster.instances["first"]
        second = cluster.instances["second"]

        a = first.query("SELECT count() from db_different_schema.source")
        b = second.query("SELECT count() from db_different_schema.destination")
        assert a == b, "Count"

        a = TSV(first.query("""SELECT sipHash64(*) from db_different_schema.source
            ORDER BY (Column1, Column2, Column3, Column4, Column5, Column6, Column7, Column8, Column9, Column10, Column11, Column12, Column13, Column14)"""))
        b = TSV(second.query("""SELECT sipHash64(*) from db_different_schema.destination
            ORDER BY (Column1, Column2, Column3, Column4, Column5, Column6, Column7, Column8, Column9, Column10, Column11, Column12, Column13, Column14)"""))
        assert a == b, "Data"


# Just simple copying, but table schema has TTL on columns
# Also table will have slightly different schema
class TaskTTL:
    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = '/clickhouse-copier/task_ttl_columns'
        self.container_task_file = "/task_ttl_columns.xml"

        for instance_name, _ in cluster.instances.items():
            instance = cluster.instances[instance_name]
            instance.copy_file_to_container(os.path.join(CURRENT_TEST_DIR, './task_ttl_columns.xml'), self.container_task_file)
            print("Copied task file to container of '{}' instance. Path {}".format(instance_name, self.container_task_file))

    def start(self):
        first = cluster.instances["first"]
        first.query("CREATE DATABASE db_ttl_columns;")
        first.query("""CREATE TABLE db_ttl_columns.source
        (
            Column1 String,
            Column2 UInt32,
            Column3 Date,
            Column4 DateTime,
            Column5 UInt16,
            Column6 String TTL now() + INTERVAL 1 MONTH,
            Column7 Decimal(3, 1) TTL now() + INTERVAL 1 MONTH,
            Column8 Tuple(Float64, Float64) TTL now() + INTERVAL 1 MONTH
        )
        ENGINE = MergeTree()
        PARTITION BY (toYYYYMMDD(Column3), Column3)
        PRIMARY KEY (Column1, Column2, Column3)
        ORDER BY (Column1, Column2, Column3)
        SETTINGS index_granularity = 8192""")

        first.query("""INSERT INTO db_ttl_columns.source SELECT * FROM generateRandom(
            'Column1 String, Column2 UInt32, Column3 Date, Column4 DateTime, Column5 UInt16,
            Column6 String, Column7 Decimal(3, 1), Column8 Tuple(Float64, Float64)', 1, 10, 2) LIMIT 50;""")

        second = cluster.instances["second"]
        second.query("CREATE DATABASE db_ttl_columns;")
        second.query("""CREATE TABLE db_ttl_columns.destination
        (
            Column1 String,
            Column2 UInt32,
            Column3 Date,
            Column4 DateTime TTL now() + INTERVAL 1 MONTH,
            Column5 UInt16 TTL now() + INTERVAL 1 MONTH,
            Column6 String TTL now() + INTERVAL 1 MONTH,
            Column7 Decimal(3, 1) TTL now() + INTERVAL 1 MONTH,
            Column8 Tuple(Float64, Float64)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(Column3)
        ORDER BY (Column3, Column2, Column1);""")
        
        print("Preparation completed")

    def check(self):
        first = cluster.instances["first"]
        second = cluster.instances["second"]

        a = first.query("SELECT count() from db_ttl_columns.source")
        b = second.query("SELECT count() from db_ttl_columns.destination")
        assert a == b, "Count"

        a = TSV(first.query("""SELECT sipHash64(*) from db_ttl_columns.source
            ORDER BY (Column1, Column2, Column3, Column4, Column5, Column6, Column7, Column8)"""))
        b = TSV(second.query("""SELECT sipHash64(*) from db_ttl_columns.destination
            ORDER BY (Column1, Column2, Column3, Column4, Column5, Column6, Column7, Column8)"""))
        assert a == b, "Data"


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
           '--task-file', task.container_task_file,
           '--task-upload-force', 'true',
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
@pytest.mark.timeout(600)
def test_different_schema(started_cluster):
    execute_task(TaskWithDifferentSchema(started_cluster), [])


@pytest.mark.timeout(600)
def test_ttl_columns(started_cluster):
    execute_task(TaskTTL(started_cluster), [])