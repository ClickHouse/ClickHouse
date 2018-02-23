import os
import os.path as p
import sys
import time
import datetime
import pytest
from contextlib import contextmanager
import docker
from kazoo.client import KazooClient


CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(CURRENT_TEST_DIR))
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

COPYING_FAIL_PROBABILITY = 0.33
cluster = None


def check_all_hosts_sucesfully_executed(tsv_content, num_hosts):
    M = TSV.toMat(tsv_content)
    hosts = [(l[0], l[1]) for l in M] # (host, port)
    codes = [l[2] for l in M]
    messages = [l[3] for l in M]

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
         "0" : {
            "0" : ["0", "1"],
            "1" : ["0"]
         },
         "1" : {
            "0" : ["0", "1"],
            "1" : ["0"]
         }
        }

        cluster = ClickHouseCluster(__file__)

        for cluster_name, shards in clusters_schema.iteritems():
            for shard_name, replicas in shards.iteritems():
                for replica_name in replicas:
                    name = "s{}_{}_{}".format(cluster_name, shard_name, replica_name)
                    cluster.add_instance(name,
                        config_dir="configs",
                        macroses={"cluster": cluster_name, "shard": shard_name, "replica": replica_name},
                        with_zookeeper=True)

        cluster.start()
        yield cluster

    finally:
        pass
        cluster.shutdown()


class Task1:

    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path="/clickhouse-copier/task_simple"
        self.copier_task_config = open(os.path.join(CURRENT_TEST_DIR, 'task0_description.xml'), 'r').read()


    def start(self):
        instance = cluster.instances['s0_0_0']

        for cluster_num in ["0", "1"]:
            ddl_check_query(instance, "DROP DATABASE IF EXISTS default ON CLUSTER cluster{}".format(cluster_num))
            ddl_check_query(instance, "CREATE DATABASE IF NOT EXISTS default ON CLUSTER cluster{}".format(cluster_num))

        ddl_check_query(instance, "CREATE TABLE hits ON CLUSTER cluster0 (d UInt64, d1 UInt64 MATERIALIZED d+1) ENGINE=ReplicatedMergeTree('/clickhouse/tables/cluster_{cluster}/{shard}/hits', '{replica}') PARTITION BY d % 3 ORDER BY d SETTINGS index_granularity = 16")
        ddl_check_query(instance, "CREATE TABLE hits_all ON CLUSTER cluster0 (d UInt64) ENGINE=Distributed(cluster0, default, hits, d)")
        ddl_check_query(instance, "CREATE TABLE hits_all ON CLUSTER cluster1 (d UInt64) ENGINE=Distributed(cluster1, default, hits, d + 1)")
        instance.query("INSERT INTO hits_all SELECT * FROM system.numbers LIMIT 1002", settings={"insert_distributed_sync": 1})


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

    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path="/clickhouse-copier/task_month_to_week_partition"
        self.copier_task_config = open(os.path.join(CURRENT_TEST_DIR, 'task_month_to_week_description.xml'), 'r').read()


    def start(self):
        instance = cluster.instances['s0_0_0']

        for cluster_num in ["0", "1"]:
            ddl_check_query(instance, "DROP DATABASE IF EXISTS default ON CLUSTER cluster{}".format(cluster_num))
            ddl_check_query(instance, "CREATE DATABASE IF NOT EXISTS default ON CLUSTER cluster{}".format(cluster_num))

        ddl_check_query(instance, "CREATE TABLE a ON CLUSTER cluster0 (date Date, d UInt64, d1 UInt64 ALIAS d+1) ENGINE=ReplicatedMergeTree('/clickhouse/tables/cluster_{cluster}/{shard}/a', '{replica}', date, intHash64(d), (date, intHash64(d)), 8192)")
        ddl_check_query(instance, "CREATE TABLE a_all ON CLUSTER cluster0 (date Date, d UInt64) ENGINE=Distributed(cluster0, default, a, d)")

        instance.query("INSERT INTO a_all SELECT toDate(17581 + number) AS date, number AS d FROM system.numbers LIMIT 85", settings={"insert_distributed_sync": 1})


    def check(self):
        assert TSV(self.cluster.instances['s0_0_0'].query("SELECT count() FROM cluster(cluster0, default, a)")) == TSV("85\n")
        assert TSV(self.cluster.instances['s1_0_0'].query("SELECT count(), uniqExact(date) FROM cluster(cluster1, default, b)")) == TSV("85\t85\n")

        assert TSV(self.cluster.instances['s1_0_0'].query("SELECT DISTINCT d % 2 FROM b")) == TSV("1\n")
        assert TSV(self.cluster.instances['s1_1_0'].query("SELECT DISTINCT d % 2 FROM b")) == TSV("0\n")

        assert TSV(self.cluster.instances['s1_0_0'].query("SELECT uniqExact(partition) IN (12, 13) FROM system.parts WHERE active AND database='default' AND table='b'")) == TSV("1\n")
        assert TSV(self.cluster.instances['s1_1_0'].query("SELECT uniqExact(partition) IN (12, 13) FROM system.parts WHERE active AND database='default' AND table='b'")) == TSV("1\n")

        instance = cluster.instances['s0_0_0']
        ddl_check_query(instance, "DROP TABLE a ON CLUSTER cluster0")
        ddl_check_query(instance, "DROP TABLE b ON CLUSTER cluster1")


def execute_task(task, cmd_options):
    task.start()

    zk = cluster.get_kazoo_client('zoo1')
    print "Use ZooKeeper server: {}:{}".format(zk.hosts[0][0], zk.hosts[0][1])

    zk_task_path = task.zk_task_path
    zk.ensure_path(zk_task_path)
    zk.create(zk_task_path + "/description", task.copier_task_config)

    # Run cluster-copier processes on each node
    docker_api = docker.from_env().api
    copiers_exec_ids = []

    cmd = ['/usr/bin/clickhouse', 'copier',
        '--config', '/etc/clickhouse-server/config-preprocessed.xml',
        '--task-path', zk_task_path,
        '--base-dir', '/var/log/clickhouse-server/copier']
    cmd += cmd_options

    for instance_name, instance in cluster.instances.iteritems():
        container = instance.get_docker_handle()
        exec_id = docker_api.exec_create(container.id, cmd, stderr=True)
        docker_api.exec_start(exec_id, detach=True)

        copiers_exec_ids.append(exec_id)
        print "Copier for {} ({}) has started".format(instance.name, instance.ip_address)

    # Wait for copiers stopping and check their return codes
    for exec_id, instance in zip(copiers_exec_ids, cluster.instances.itervalues()):
        while True:
            res = docker_api.exec_inspect(exec_id)
            if not res['Running']:
                break
            time.sleep(1)

        assert res['ExitCode'] == 0, "Instance: {} ({}). Info: {}".format(instance.name, instance.ip_address, repr(res))

    try:
        task.check()
    finally:
        zk.delete(zk_task_path, recursive=True)


def test_copy1_simple(started_cluster):
    execute_task(Task1(started_cluster), [])


def test_copy1_with_recovering(started_cluster):
    execute_task(Task1(started_cluster), ['--copy-fault-probability', str(COPYING_FAIL_PROBABILITY)])


def test_copy_month_to_week_partition(started_cluster):
    execute_task(Task2(started_cluster), [])

def test_copy_month_to_week_partition(started_cluster):
    execute_task(Task2(started_cluster), ['--copy-fault-probability', str(0.1)])

if __name__ == '__main__':
    with contextmanager(started_cluster)() as cluster:
       for name, instance in cluster.instances.items():
           print name, instance.ip_address
       raw_input("Cluster created, press any key to destroy...")
