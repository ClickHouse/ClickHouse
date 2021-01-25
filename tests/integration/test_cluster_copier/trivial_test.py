import os
import sys
import time
from contextlib import contextmanager

import docker
import pytest

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(CURRENT_TEST_DIR))
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

COPYING_FAIL_PROBABILITY = 0.33
MOVING_FAIL_PROBABILITY = 0.1
cluster = None


@pytest.fixture(scope="function")
def started_cluster():
    global cluster
    try:
        clusters_schema = {
            "0": {"0": ["0"]},
            "1": {"0": ["0"]}
        }

        cluster = ClickHouseCluster(__file__)

        for cluster_name, shards in clusters_schema.items():
            for shard_name, replicas in shards.items():
                for replica_name in replicas:
                    name = "s{}_{}_{}".format(cluster_name, shard_name, replica_name)
                    cluster.add_instance(name,
                                         main_configs=[], user_configs=[],
                                         macros={"cluster": cluster_name, "shard": shard_name, "replica": replica_name},
                                         with_zookeeper=True)

        cluster.start()
        yield cluster

    finally:
        pass
        cluster.shutdown()


class TaskTrivial:
    def __init__(self, cluster, use_sample_offset):
        self.cluster = cluster
        if use_sample_offset:
            self.zk_task_path = "/clickhouse-copier/task_trivial_use_sample_offset"
        else:
            self.zk_task_path = "/clickhouse-copier/task_trivial"
        self.copier_task_config = open(os.path.join(CURRENT_TEST_DIR, 'task_trivial.xml'), 'r').read()

    def start(self):
        source = cluster.instances['s0_0_0']
        destination = cluster.instances['s1_0_0']

        for node in [source, destination]:
            node.query("DROP DATABASE IF EXISTS default")
            node.query("CREATE DATABASE IF NOT EXISTS default")

        source.query("CREATE TABLE trivial (d UInt64, d1 UInt64 MATERIALIZED d+1) "
                     "ENGINE=ReplicatedMergeTree('/clickhouse/tables/source_trivial_cluster/1/trivial', '1') "
                     "PARTITION BY d % 5 ORDER BY (d, sipHash64(d)) SAMPLE BY sipHash64(d) SETTINGS index_granularity = 16")

        source.query("INSERT INTO trivial SELECT * FROM system.numbers LIMIT 1002",
                     settings={"insert_distributed_sync": 1})

    def check(self):
        source = cluster.instances['s0_0_0']
        destination = cluster.instances['s1_0_0']

        assert TSV(source.query("SELECT count() FROM trivial")) == TSV("1002\n")
        assert TSV(destination.query("SELECT count() FROM trivial")) == TSV("1002\n")

        for node in [source, destination]:
            node.query("DROP TABLE trivial")


def execute_task(task, cmd_options):
    task.start()

    zk = cluster.get_kazoo_client('zoo1')
    print("Use ZooKeeper server: {}:{}".format(zk.hosts[0][0], zk.hosts[0][1]))

    zk_task_path = task.zk_task_path
    zk.ensure_path(zk_task_path)
    zk.create(zk_task_path + "/description", task.copier_task_config)

    # Run cluster-copier processes on each node
    docker_api = docker.from_env().api
    copiers_exec_ids = []

    cmd = ['/usr/bin/clickhouse', 'copier',
           '--config', '/etc/clickhouse-server/config-copier.xml',
           '--task-path', zk_task_path,
           '--base-dir', '/var/log/clickhouse-server/copier']
    cmd += cmd_options

    print(cmd)

    for instance_name, instance in cluster.instances.items():
        container = instance.get_docker_handle()
        exec_id = docker_api.exec_create(container.id, cmd, stderr=True)
        docker_api.exec_start(exec_id, detach=True)

        copiers_exec_ids.append(exec_id)
        print("Copier for {} ({}) has started".format(instance.name, instance.ip_address))

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
        zk.delete(zk_task_path, recursive=True)


# Tests


@pytest.mark.parametrize(
    ('use_sample_offset'),
    [
        False,
        True
    ]
)
def test_trivial_copy(started_cluster, use_sample_offset):
    if use_sample_offset:
        execute_task(TaskTrivial(started_cluster, use_sample_offset), ['--experimental-use-sample-offset', '1'])
    else:
        print("AAAAA")
        execute_task(TaskTrivial(started_cluster, use_sample_offset), [])


@pytest.mark.parametrize(
    ('use_sample_offset'),
    [
        False,
        True
    ]
)
def test_trivial_copy_with_copy_fault(started_cluster, use_sample_offset):
    if use_sample_offset:
        execute_task(TaskTrivial(started_cluster), ['--copy-fault-probability', str(COPYING_FAIL_PROBABILITY),
                                                    '--experimental-use-sample-offset', '1'])
    else:
        execute_task(TaskTrivial(started_cluster), ['--copy-fault-probability', str(COPYING_FAIL_PROBABILITY)])


@pytest.mark.parametrize(
    ('use_sample_offset'),
    [
        False,
        True
    ]
)
def test_trivial_copy_with_move_fault(started_cluster, use_sample_offset):
    if use_sample_offset:
        execute_task(TaskTrivial(started_cluster), ['--move-fault-probability', str(MOVING_FAIL_PROBABILITY),
                                                    '--experimental-use-sample-offset', '1'])
    else:
        execute_task(TaskTrivial(started_cluster), ['--move-fault-probability', str(MOVING_FAIL_PROBABILITY)])


if __name__ == '__main__':
    with contextmanager(started_cluster)() as cluster:
        for name, instance in list(cluster.instances.items()):
            print(name, instance.ip_address)
        input("Cluster created, press any key to destroy...")
