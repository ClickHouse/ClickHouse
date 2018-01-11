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

    instance.query("CREATE TABLE hits ON CLUSTER cluster0 (d UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/cluster{cluster}/{shard}', '{replica}') PARTITION BY d % 3 ORDER BY d SETTINGS index_granularity = 16")
    instance.query("CREATE TABLE hits_all ON CLUSTER cluster0 (d UInt64) ENGINE=Distributed(cluster0, default, hits, d)")
    instance.query("CREATE TABLE hits_all ON CLUSTER cluster1 (d UInt64) ENGINE=Distributed(cluster1, default, hits, d + 1)")
    instance.query("INSERT INTO hits_all SELECT * FROM system.numbers LIMIT 1002")

    zoo_id = cluster.get_instance_docker_id('zoo1')
    zoo_handle = cluster.docker_client.containers.get(zoo_id)
    zoo_ip = zoo_handle.attrs['NetworkSettings']['Networks'].values()[0]['IPAddress']
    print "Use ZooKeeper server: {} ({})".format(zoo_id, zoo_ip)

    zk = KazooClient(hosts=zoo_ip)
    zk.start()
    zk_task_path = "/clickhouse-copier/task_simple"
    zk.ensure_path(zk_task_path)
    copier_task_config = open(os.path.join(CURRENT_TEST_DIR, 'task0_description.xml'), 'r').read()
    zk.create(zk_task_path + "/description", copier_task_config)

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

    # Wait for copiers finalizing and check their return codes
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
    instance.query("DROP TABLE hits_all ON CLUSTER cluster1")
    instance.query("DROP TABLE hits_all ON CLUSTER cluster1")
    instance.query("DROP TABLE hits ON CLUSTER cluster0")
    instance.query("DROP TABLE hits ON CLUSTER cluster1")


def test_copy_simple(started_cluster):
    _test_copying([])


def test_copy_with_recovering(started_cluster):
    _test_copying(['--copy-fault-probability', str(COPYING_FAIL_PROBABILITY)])


if __name__ == '__main__':
    with contextmanager(started_cluster)() as cluster:
       for name, instance in cluster.instances.items():
           print name, instance.ip_address
       raw_input("Cluster created, press any key to destroy...")
