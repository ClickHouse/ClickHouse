import os
import random
import sys
import time
from contextlib import contextmanager

import docker
import kazoo
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(CURRENT_TEST_DIR))

COPYING_FAIL_PROBABILITY = 0.2
MOVING_FAIL_PROBABILITY = 0.2

cluster = ClickHouseCluster(__file__)


def check_all_hosts_sucesfully_executed(tsv_content, num_hosts):
    M = TSV.toMat(tsv_content)
    hosts = [(l[0], l[1]) for l in M]  # (host, port)
    codes = [l[2] for l in M]

    assert len(hosts) == num_hosts and len(set(hosts)) == num_hosts, "\n" + tsv_content
    assert len(set(codes)) == 1, "\n" + tsv_content
    assert codes[0] == "0", "\n" + tsv_content


def ddl_check_query(instance, query, num_hosts=3):
    contents = instance.query(query)
    check_all_hosts_sucesfully_executed(contents, num_hosts)
    return contents


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        clusters_schema = {
            "0": {
                "0": ["0", "1"],
                "1": ["0"]
            },
            "1": {
                "0": ["0", "1"],
                "1": ["0"]
            }
        }

        for cluster_name, shards in clusters_schema.items():
            for shard_name, replicas in shards.items():
                for replica_name in replicas:
                    name = "s{}_{}_{}".format(cluster_name, shard_name, replica_name)
                    cluster.add_instance(name,
                                         main_configs=["configs/conf.d/query_log.xml", "configs/conf.d/ddl.xml",
                                                       "configs/conf.d/clusters.xml"],
                                         user_configs=["configs/users.xml"],
                                         macros={"cluster": cluster_name, "shard": shard_name, "replica": replica_name},
                                         with_zookeeper=True)

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


class Task1:

    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = "/clickhouse-copier/task_simple"
        self.copier_task_config = open(os.path.join(CURRENT_TEST_DIR, 'task0_description.xml'), 'r').read()

    def start(self):
        instance = cluster.instances['s0_0_0']

        for cluster_num in ["0", "1"]:
            ddl_check_query(instance, "DROP DATABASE IF EXISTS default ON CLUSTER cluster{}".format(cluster_num))
            ddl_check_query(instance,
                            "CREATE DATABASE IF NOT EXISTS default ON CLUSTER cluster{}".format(
                                cluster_num))

        ddl_check_query(instance, "CREATE TABLE hits ON CLUSTER cluster0 (d UInt64, d1 UInt64 MATERIALIZED d+1) " +
                        "ENGINE=ReplicatedMergeTree " +
                        "PARTITION BY d % 3 ORDER BY (d, sipHash64(d)) SAMPLE BY sipHash64(d) SETTINGS index_granularity = 16")
        ddl_check_query(instance,
                        "CREATE TABLE hits_all ON CLUSTER cluster0 (d UInt64) ENGINE=Distributed(cluster0, default, hits, d)")
        ddl_check_query(instance,
                        "CREATE TABLE hits_all ON CLUSTER cluster1 (d UInt64) ENGINE=Distributed(cluster1, default, hits, d + 1)")
        instance.query("INSERT INTO hits_all SELECT * FROM system.numbers LIMIT 1002",
                       settings={"insert_distributed_sync": 1})

    def check(self):
        assert TSV(self.cluster.instances['s0_0_0'].query("SELECT count() FROM hits_all")) == TSV("1002\n")
        assert TSV(self.cluster.instances['s1_0_0'].query("SELECT count() FROM hits_all")) == TSV("1002\n")

        assert TSV(self.cluster.instances['s1_0_0'].query("SELECT DISTINCT d % 2 FROM hits")) == TSV("1\n")
        assert TSV(self.cluster.instances['s1_1_0'].query("SELECT DISTINCT d % 2 FROM hits")) == TSV("0\n")

        instance = self.cluster.instances['s0_0_0']
        ddl_check_query(instance, "DROP TABLE hits_all ON CLUSTER cluster0")
        ddl_check_query(instance, "DROP TABLE hits_all ON CLUSTER cluster1")
        ddl_check_query(instance, "DROP TABLE hits ON CLUSTER cluster0")
        ddl_check_query(instance, "DROP TABLE hits ON CLUSTER cluster1")


class Task2:

    def __init__(self, cluster, unique_zk_path):
        self.cluster = cluster
        self.zk_task_path = "/clickhouse-copier/task_month_to_week_partition"
        self.copier_task_config = open(os.path.join(CURRENT_TEST_DIR, 'task_month_to_week_description.xml'), 'r').read()
        self.unique_zk_path = unique_zk_path

    def start(self):
        instance = cluster.instances['s0_0_0']

        for cluster_num in ["0", "1"]:
            ddl_check_query(instance, "DROP DATABASE IF EXISTS default ON CLUSTER cluster{}".format(cluster_num))
            ddl_check_query(instance,
                            "CREATE DATABASE IF NOT EXISTS default ON CLUSTER cluster{}".format(
                                cluster_num))

        ddl_check_query(instance,
                        "CREATE TABLE a ON CLUSTER cluster0 (date Date, d UInt64, d1 UInt64 ALIAS d+1) "
                        "ENGINE=ReplicatedMergeTree('/clickhouse/tables/cluster_{cluster}/{shard}/" + self.unique_zk_path + "', "
                                                   "'{replica}', date, intHash64(d), (date, intHash64(d)), 8192)")
        ddl_check_query(instance,
                        "CREATE TABLE a_all ON CLUSTER cluster0 (date Date, d UInt64) ENGINE=Distributed(cluster0, default, a, d)")

        instance.query(
            "INSERT INTO a_all SELECT toDate(17581 + number) AS date, number AS d FROM system.numbers LIMIT 85",
            settings={"insert_distributed_sync": 1})

    def check(self):
        assert TSV(self.cluster.instances['s0_0_0'].query("SELECT count() FROM cluster(cluster0, default, a)")) == TSV(
            "85\n")
        assert TSV(self.cluster.instances['s1_0_0'].query(
            "SELECT count(), uniqExact(date) FROM cluster(cluster1, default, b)")) == TSV("85\t85\n")

        assert TSV(self.cluster.instances['s1_0_0'].query(
            "SELECT DISTINCT jumpConsistentHash(intHash64(d), 2) FROM b")) == TSV("0\n")
        assert TSV(self.cluster.instances['s1_1_0'].query(
            "SELECT DISTINCT jumpConsistentHash(intHash64(d), 2) FROM b")) == TSV("1\n")

        assert TSV(self.cluster.instances['s1_0_0'].query(
            "SELECT uniqExact(partition) IN (12, 13) FROM system.parts WHERE active AND database='default' AND table='b'")) == TSV(
            "1\n")
        assert TSV(self.cluster.instances['s1_1_0'].query(
            "SELECT uniqExact(partition) IN (12, 13) FROM system.parts WHERE active AND database='default' AND table='b'")) == TSV(
            "1\n")

        instance = cluster.instances['s0_0_0']
        ddl_check_query(instance, "DROP TABLE a ON CLUSTER cluster0")
        ddl_check_query(instance, "DROP TABLE b ON CLUSTER cluster1")


class Task_test_block_size:

    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = "/clickhouse-copier/task_test_block_size"
        self.copier_task_config = open(os.path.join(CURRENT_TEST_DIR, 'task_test_block_size.xml'), 'r').read()
        self.rows = 1000000

    def start(self):
        instance = cluster.instances['s0_0_0']

        ddl_check_query(instance, """
            CREATE TABLE test_block_size ON CLUSTER shard_0_0 (partition Date, d UInt64)
            ENGINE=ReplicatedMergeTree
            ORDER BY (d, sipHash64(d)) SAMPLE BY sipHash64(d)""", 2)

        instance.query(
            "INSERT INTO test_block_size SELECT toDate(0) AS partition, number as d FROM system.numbers LIMIT {}".format(
                self.rows))

    def check(self):
        assert TSV(self.cluster.instances['s1_0_0'].query(
            "SELECT count() FROM cluster(cluster1, default, test_block_size)")) == TSV("{}\n".format(self.rows))

        instance = cluster.instances['s0_0_0']
        ddl_check_query(instance, "DROP TABLE test_block_size ON CLUSTER shard_0_0", 2)
        ddl_check_query(instance, "DROP TABLE test_block_size ON CLUSTER cluster1")


class Task_no_index:

    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = "/clickhouse-copier/task_no_index"
        self.copier_task_config = open(os.path.join(CURRENT_TEST_DIR, 'task_no_index.xml'), 'r').read()
        self.rows = 1000000

    def start(self):
        instance = cluster.instances['s0_0_0']
        instance.query("create table ontime (Year UInt16, FlightDate String) ENGINE = Memory")
        instance.query("insert into ontime values (2016, 'test6'), (2017, 'test7'), (2018, 'test8')")

    def check(self):
        assert TSV(self.cluster.instances['s1_1_0'].query("SELECT Year FROM ontime22")) == TSV("2017\n")
        instance = cluster.instances['s0_0_0']
        instance.query("DROP TABLE ontime")
        instance = cluster.instances['s1_1_0']
        instance.query("DROP TABLE ontime22")


class Task_no_arg:

    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = "/clickhouse-copier/task_no_arg"
        self.copier_task_config = open(os.path.join(CURRENT_TEST_DIR, 'task_no_arg.xml'), 'r').read()
        self.rows = 1000000

    def start(self):
        instance = cluster.instances['s0_0_0']
        instance.query(
            "create table copier_test1 (date Date, id UInt32) engine = MergeTree PARTITION BY date ORDER BY date SETTINGS index_granularity = 8192")
        instance.query("insert into copier_test1 values ('2016-01-01', 10);")

    def check(self):
        assert TSV(self.cluster.instances['s1_1_0'].query("SELECT date FROM copier_test1_1")) == TSV("2016-01-01\n")
        instance = cluster.instances['s0_0_0']
        instance.query("DROP TABLE copier_test1")
        instance = cluster.instances['s1_1_0']
        instance.query("DROP TABLE copier_test1_1")

class Task_non_partitioned_table:

    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = "/clickhouse-copier/task_non_partitoned_table"
        self.copier_task_config = open(os.path.join(CURRENT_TEST_DIR, 'task_non_partitioned_table.xml'), 'r').read()
        self.rows = 1000000

    def start(self):
        instance = cluster.instances['s0_0_0']
        instance.query(
            "create table copier_test1 (date Date, id UInt32) engine = MergeTree ORDER BY date SETTINGS index_granularity = 8192")
        instance.query("insert into copier_test1 values ('2016-01-01', 10);")

    def check(self):
        assert TSV(self.cluster.instances['s1_1_0'].query("SELECT date FROM copier_test1_1")) == TSV("2016-01-01\n")
        instance = cluster.instances['s0_0_0']
        instance.query("DROP TABLE copier_test1")
        instance = cluster.instances['s1_1_0']
        instance.query("DROP TABLE copier_test1_1")


def execute_task(task, cmd_options):
    task.start()

    zk = cluster.get_kazoo_client('zoo1')
    print("Use ZooKeeper server: {}:{}".format(zk.hosts[0][0], zk.hosts[0][1]))

    try:
        zk.delete("/clickhouse-copier", recursive=True)
    except kazoo.exceptions.NoNodeError:
        print("No node /clickhouse-copier. It is Ok in first test.")

    zk_task_path = task.zk_task_path
    zk.ensure_path(zk_task_path)
    zk.create(zk_task_path + "/description", task.copier_task_config.encode())

    # Run cluster-copier processes on each node
    docker_api = docker.from_env().api
    copiers_exec_ids = []

    cmd = ['/usr/bin/clickhouse', 'copier',
           '--config', '/etc/clickhouse-server/config-copier.xml',
           '--task-path', zk_task_path,
           '--base-dir', '/var/log/clickhouse-server/copier']
    cmd += cmd_options

    copiers = random.sample(list(cluster.instances.keys()), 3)

    for instance_name in copiers:
        instance = cluster.instances[instance_name]
        container = instance.get_docker_handle()
        instance.copy_file_to_container(os.path.join(CURRENT_TEST_DIR, "configs/config-copier.xml"),
                                        "/etc/clickhouse-server/config-copier.xml")
        print("Copied copier config to {}".format(instance.name))
        exec_id = docker_api.exec_create(container.id, cmd, stderr=True)
        output = docker_api.exec_start(exec_id).decode('utf8')
        print(output)
        copiers_exec_ids.append(exec_id)
        print("Copier for {} ({}) has started".format(instance.name, instance.ip_address))

    # Wait for copiers stopping and check their return codes
    for exec_id, instance_name in zip(copiers_exec_ids, copiers):
        instance = cluster.instances[instance_name]
        while True:
            res = docker_api.exec_inspect(exec_id)
            if not res['Running']:
                break
            time.sleep(0.5)

        assert res['ExitCode'] == 0, "Instance: {} ({}). Info: {}".format(instance.name, instance.ip_address, repr(res))

    try:
        task.check()
    finally:
        zk.delete(zk_task_path, recursive=True)


# Tests

@pytest.mark.parametrize(
    ('use_sample_offset'),
    [
        False,
        True
    ]
)
def test_copy_simple(started_cluster, use_sample_offset):
    if use_sample_offset:
        execute_task(Task1(started_cluster), ['--experimental-use-sample-offset', '1'])
    else:
        execute_task(Task1(started_cluster), [])


@pytest.mark.parametrize(
    ('use_sample_offset'),
    [
        False,
        True
    ]
)
def test_copy_with_recovering(started_cluster, use_sample_offset):
    if use_sample_offset:
        execute_task(Task1(started_cluster), ['--copy-fault-probability', str(COPYING_FAIL_PROBABILITY),
                                              '--experimental-use-sample-offset', '1'])
    else:
        execute_task(Task1(started_cluster), ['--copy-fault-probability', str(COPYING_FAIL_PROBABILITY)])


@pytest.mark.parametrize(
    ('use_sample_offset'),
    [
        False,
        True
    ]
)
def test_copy_with_recovering_after_move_faults(started_cluster, use_sample_offset):
    if use_sample_offset:
        execute_task(Task1(started_cluster), ['--move-fault-probability', str(MOVING_FAIL_PROBABILITY),
                                              '--experimental-use-sample-offset', '1'])
    else:
        execute_task(Task1(started_cluster), ['--move-fault-probability', str(MOVING_FAIL_PROBABILITY)])


@pytest.mark.timeout(600)
def test_copy_month_to_week_partition(started_cluster):
    execute_task(Task2(started_cluster, "test1"), [])


@pytest.mark.timeout(600)
def test_copy_month_to_week_partition_with_recovering(started_cluster):
    execute_task(Task2(started_cluster, "test2"), ['--copy-fault-probability', str(COPYING_FAIL_PROBABILITY)])


@pytest.mark.timeout(600)
def test_copy_month_to_week_partition_with_recovering_after_move_faults(started_cluster):
    execute_task(Task2(started_cluster, "test3"), ['--move-fault-probability', str(MOVING_FAIL_PROBABILITY)])


def test_block_size(started_cluster):
    execute_task(Task_test_block_size(started_cluster), [])


def test_no_index(started_cluster):
    execute_task(Task_no_index(started_cluster), [])


def test_no_arg(started_cluster):
    execute_task(Task_no_arg(started_cluster), [])

def test_non_partitioned_table(started_cluster):
    execute_task(Task_non_partitioned_table(started_cluster), [])

if __name__ == '__main__':
    with contextmanager(started_cluster)() as cluster:
        for name, instance in list(cluster.instances.items()):
            print(name, instance.ip_address)
        input("Cluster created, press any key to destroy...")
