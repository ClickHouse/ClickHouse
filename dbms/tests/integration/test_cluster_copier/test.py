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


def _test_copying(cmd_options):
    instance = cluster.instances['s0_0_0']

    ddl_check_query(instance, "DROP TABLE IF EXISTS hits ON CLUSTER cluster0")
    ddl_check_query(instance, "DROP TABLE IF EXISTS hits ON CLUSTER cluster1")

    ddl_check_query(instance, "CREATE TABLE hits ON CLUSTER cluster0 (d UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/cluster_{cluster}/{shard}', '{replica}') PARTITION BY d % 3 ORDER BY d SETTINGS index_granularity = 16")
    ddl_check_query(instance, "CREATE TABLE hits_all ON CLUSTER cluster0 (d UInt64) ENGINE=Distributed(cluster0, default, hits, d)")
    ddl_check_query(instance, "CREATE TABLE hits_all ON CLUSTER cluster1 (d UInt64) ENGINE=Distributed(cluster1, default, hits, d + 1)")
    instance.query("INSERT INTO hits_all SELECT * FROM system.numbers LIMIT 1002")

    zk = cluster.get_kazoo_client('zoo1')
    print "Use ZooKeeper server: {}:{}".format(zk.hosts[0][0], zk.hosts[0][1])

    zk_task_path = "/clickhouse-copier/task_simple"
    zk.ensure_path(zk_task_path)

    copier_task_config = open(os.path.join(CURRENT_TEST_DIR, 'task0_description.xml'), 'r').read()
    zk.create(zk_task_path + "/description", copier_task_config)

    # Run cluster-copier processes on each node
    docker_api = docker.from_env().api
    copiers_exec_ids = []

    cmd = ['/usr/bin/clickhouse', 'copier',
        '--config', '/etc/clickhouse-server/config-preprocessed.xml',
        '--task-path', '/clickhouse-copier/task_simple',
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

    assert TSV(cluster.instances['s0_0_0'].query("SELECT count() FROM hits_all")) == TSV("1002\n")
    assert TSV(cluster.instances['s1_0_0'].query("SELECT count() FROM hits_all")) == TSV("1002\n")

    assert TSV(cluster.instances['s1_0_0'].query("SELECT DISTINCT d % 2 FROM hits")) == TSV("1\n")
    assert TSV(cluster.instances['s1_1_0'].query("SELECT DISTINCT d % 2 FROM hits")) == TSV("0\n")

    zk.delete(zk_task_path, recursive=True)
    ddl_check_query(instance, "DROP TABLE hits_all ON CLUSTER cluster0")
    ddl_check_query(instance, "DROP TABLE hits_all ON CLUSTER cluster1")
    ddl_check_query(instance, "DROP TABLE hits ON CLUSTER cluster0")
    ddl_check_query(instance, "DROP TABLE hits ON CLUSTER cluster1")


def test_copy_simple(started_cluster):
    _test_copying([])


def test_copy_with_recovering(started_cluster):
    _test_copying(['--copy-fault-probability', str(COPYING_FAIL_PROBABILITY)])


if __name__ == '__main__':
    with contextmanager(started_cluster)() as cluster:
       for name, instance in cluster.instances.items():
           print name, instance.ip_address
       raw_input("Cluster created, press any key to destroy...")
