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
                "1": ["0", "1"],
                "2": ["0", "1"],
                "3": ["0", "1"],
                "4": ["0", "1"]
            },
            "1": {
                "0": ["0"]
            }
        }

        for cluster_name, shards in clusters_schema.items():
            for shard_name, replicas in shards.items():
                for replica_name in replicas:
                    name = "s{}_{}_{}".format(cluster_name, shard_name, replica_name)
                    cluster.add_instance(name,
                                            main_configs=["configs/conf.d/query_log.xml", "configs/conf.d/ddl.xml",
                                                        "configs/conf.d/big_clusters.xml"],
                                            user_configs=["configs/users.xml"],
                                            macros={"cluster": cluster_name, "shard": shard_name, "replica": replica_name},
                                            with_zookeeper=True)

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


class Task_many_to_one:
    def __init__(self, cluster):
        self.cluster = cluster 
        self.zk_task_path = "/clickhouse-copier/task_many_to_one"
        self.copier_task_config = open(os.path.join(CURRENT_TEST_DIR, 'task_many_to_one.xml'), 'r').read()
        self.rows = 1000

    def start(self):
        instance = cluster.instances['s0_0_0']

        for cluster_num in ["big_source", "big_destination"]:
            instance.query("DROP DATABASE IF EXISTS default ON CLUSTER {}".format(cluster_num))
            instance.query("CREATE DATABASE IF NOT EXISTS default ON CLUSTER {}".format(cluster_num))

        instance.query("CREATE TABLE big ON CLUSTER big_source (first_id UUID DEFAULT generateUUIDv4(), second_id UInt64, datetime DateTime DEFAULT now()) " +
                       "ENGINE=ReplicatedMergeTree " +
                       "PARTITION BY toSecond(datetime) " +  
                       "ORDER BY (first_id, second_id, toSecond(datetime))")
        instance.query("CREATE TABLE big_all ON CLUSTER big_source (first_id UUID, second_id UInt64, datetime DateTime) ENGINE=Distributed(big_source, default, big, rand() % 5)")
        instance.query("INSERT INTO big_all SELECT generateUUIDv4(), number, now() FROM system.numbers LIMIT 1002",
                       settings={"insert_distributed_sync": 1})

    def check(self):
        assert TSV(self.cluster.instances['s0_0_0'].query("SELECT count() FROM big_all")) == TSV("1002\n")
        assert TSV(self.cluster.instances['s1_0_0'].query("SELECT count() FROM big")) == TSV("1002\n")

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

def test_copy_simple(started_cluster):
    execute_task(Task_many_to_one(started_cluster), [])
 