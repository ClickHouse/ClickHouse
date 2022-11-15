import os
import sys
import time
import random
import string

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

import kazoo
import pytest
import docker


CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(CURRENT_TEST_DIR))


COPYING_FAIL_PROBABILITY = 0.1
MOVING_FAIL_PROBABILITY = 0.1

cluster = ClickHouseCluster(__file__)


def generateRandomString(count):
    return "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(count)
    )


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        for name in ["first_trivial", "second_trivial"]:
            instance = cluster.add_instance(
                name,
                main_configs=["configs/conf.d/clusters_trivial.xml"],
                user_configs=["configs_two_nodes/users.xml"],
                macros={
                    "cluster": name,
                    "shard": "the_only_shard",
                    "replica": "the_only_replica",
                },
                with_zookeeper=True,
            )

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


class TaskTrivial:
    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = "/clickhouse-copier/task_trivial"
        self.copier_task_config = open(
            os.path.join(CURRENT_TEST_DIR, "task_trivial.xml"), "r"
        ).read()

    def start(self):
        source = cluster.instances["first_trivial"]
        destination = cluster.instances["second_trivial"]

        for node in [source, destination]:
            node.query("DROP DATABASE IF EXISTS default")
            node.query("CREATE DATABASE IF NOT EXISTS default")

        source.query(
            "CREATE TABLE trivial (d UInt64, d1 UInt64 MATERIALIZED d+1)"
            "ENGINE=ReplicatedMergeTree('/clickhouse/tables/source_trivial_cluster/1/trivial/{}', '1') "
            "PARTITION BY d % 5 ORDER BY (d, sipHash64(d)) SAMPLE BY sipHash64(d) SETTINGS index_granularity = 16".format(
                generateRandomString(10)
            )
        )

        source.query(
            "INSERT INTO trivial SELECT * FROM system.numbers LIMIT 1002",
            settings={"insert_distributed_sync": 1},
        )

    def check(self):
        zk = cluster.get_kazoo_client("zoo1")
        status_data, _ = zk.get(self.zk_task_path + "/status")
        assert (
            status_data
            == b'{"hits":{"all_partitions_count":5,"processed_partitions_count":5}}'
        )

        source = cluster.instances["first_trivial"]
        destination = cluster.instances["second_trivial"]

        assert TSV(source.query("SELECT count() FROM trivial")) == TSV("1002\n")
        assert TSV(destination.query("SELECT count() FROM trivial")) == TSV("1002\n")

        for node in [source, destination]:
            node.query("DROP TABLE trivial")


class TaskReplicatedWithoutArguments:
    def __init__(self, cluster):
        self.cluster = cluster
        self.zk_task_path = "/clickhouse-copier/task_trivial_without_arguments"
        self.copier_task_config = open(
            os.path.join(CURRENT_TEST_DIR, "task_trivial_without_arguments.xml"), "r"
        ).read()

    def start(self):
        source = cluster.instances["first_trivial"]
        destination = cluster.instances["second_trivial"]

        for node in [source, destination]:
            node.query("DROP DATABASE IF EXISTS default")
            node.query("CREATE DATABASE IF NOT EXISTS default")

        source.query(
            "CREATE TABLE trivial_without_arguments ON CLUSTER source_trivial_cluster (d UInt64, d1 UInt64 MATERIALIZED d+1) "
            "ENGINE=ReplicatedMergeTree() "
            "PARTITION BY d % 5 ORDER BY (d, sipHash64(d)) SAMPLE BY sipHash64(d) SETTINGS index_granularity = 16"
        )

        source.query(
            "INSERT INTO trivial_without_arguments SELECT * FROM system.numbers LIMIT 1002",
            settings={"insert_distributed_sync": 1},
        )

    def check(self):
        zk = cluster.get_kazoo_client("zoo1")
        status_data, _ = zk.get(self.zk_task_path + "/status")
        assert (
            status_data
            == b'{"hits":{"all_partitions_count":5,"processed_partitions_count":5}}'
        )

        source = cluster.instances["first_trivial"]
        destination = cluster.instances["second_trivial"]

        assert TSV(
            source.query("SELECT count() FROM trivial_without_arguments")
        ) == TSV("1002\n")
        assert TSV(
            destination.query("SELECT count() FROM trivial_without_arguments")
        ) == TSV("1002\n")

        for node in [source, destination]:
            node.query("DROP TABLE trivial_without_arguments")


def execute_task(started_cluster, task, cmd_options):
    task.start()

    zk = started_cluster.get_kazoo_client("zoo1")
    print("Use ZooKeeper server: {}:{}".format(zk.hosts[0][0], zk.hosts[0][1]))

    try:
        zk.delete("/clickhouse-copier", recursive=True)
    except kazoo.exceptions.NoNodeError:
        print("No node /clickhouse-copier. It is Ok in first test.")

    zk_task_path = task.zk_task_path
    zk.ensure_path(zk_task_path)
    zk.create(zk_task_path + "/description", task.copier_task_config.encode())

    # Run cluster-copier processes on each node
    docker_api = started_cluster.docker_client.api
    copiers_exec_ids = []

    cmd = [
        "/usr/bin/clickhouse",
        "copier",
        "--config",
        "/etc/clickhouse-server/config-copier.xml",
        "--task-path",
        zk_task_path,
        "--base-dir",
        "/var/log/clickhouse-server/copier",
    ]
    cmd += cmd_options

    copiers = list(started_cluster.instances.keys())

    for instance_name in copiers:
        instance = started_cluster.instances[instance_name]
        container = instance.get_docker_handle()
        instance.copy_file_to_container(
            os.path.join(CURRENT_TEST_DIR, "configs/config-copier.xml"),
            "/etc/clickhouse-server/config-copier.xml",
        )
        print("Copied copier config to {}".format(instance.name))
        exec_id = docker_api.exec_create(container.id, cmd, stderr=True)
        output = docker_api.exec_start(exec_id).decode("utf8")
        print(output)
        copiers_exec_ids.append(exec_id)
        print(
            "Copier for {} ({}) has started".format(instance.name, instance.ip_address)
        )

    # Wait for copiers stopping and check their return codes
    for exec_id, instance_name in zip(copiers_exec_ids, copiers):
        instance = started_cluster.instances[instance_name]
        while True:
            res = docker_api.exec_inspect(exec_id)
            if not res["Running"]:
                break
            time.sleep(0.5)

        assert res["ExitCode"] == 0, "Instance: {} ({}). Info: {}".format(
            instance.name, instance.ip_address, repr(res)
        )

    try:
        task.check()
    finally:
        zk.delete(zk_task_path, recursive=True)


# Tests


def test_trivial_copy(started_cluster):
    execute_task(started_cluster, TaskTrivial(started_cluster), [])


def test_trivial_without_arguments(started_cluster):
    execute_task(started_cluster, TaskReplicatedWithoutArguments(started_cluster), [])
