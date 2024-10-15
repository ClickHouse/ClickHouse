#!/usr/bin/env python3

import os
import random
import string
import time
from multiprocessing.dummy import Pool

import pytest
from kazoo.client import KazooClient, KazooRetry
from kazoo.handlers.threading import KazooTimeoutError

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/keeper_config1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/keeper_config2.xml"], stay_alive=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/keeper_config3.xml"], stay_alive=True
)


def start_zookeeper(node):
    node.exec_in_container(["bash", "-c", "/opt/zookeeper/bin/zkServer.sh start"])


def stop_zookeeper(node):
    node.exec_in_container(["bash", "-c", "/opt/zookeeper/bin/zkServer.sh stop"])
    timeout = time.time() + 60
    while node.get_process_pid("zookeeper") != None:
        if time.time() > timeout:
            raise Exception("Failed to stop ZooKeeper in 60 secs")
        time.sleep(0.2)


def generate_zk_snapshot(node):
    for _ in range(100):
        stop_zookeeper(node)
        start_zookeeper(node)
        time.sleep(2)
        stop_zookeeper(node)

        # get last snapshot
        last_snapshot = node.exec_in_container(
            [
                "bash",
                "-c",
                "find /zookeeper/version-2 -name 'snapshot.*' -printf '%T@ %p\n' | sort -n | awk 'END {print $2}'",
            ]
        ).strip()

        print(f"Latest snapshot: {last_snapshot}")

        try:
            # verify last snapshot
            # zkSnapShotToolkit is a tool to inspect generated snapshots - if it's broken, an exception is thrown
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"/opt/zookeeper/bin/zkSnapShotToolkit.sh {last_snapshot}",
                ]
            )
            return
        except Exception as err:
            print(f"Got error while reading snapshot: {err}")

    raise Exception("Failed to generate a ZooKeeper snapshot")


def clear_zookeeper(node):
    node.exec_in_container(["bash", "-c", "rm -fr /zookeeper/*"])


def restart_and_clear_zookeeper(node):
    stop_zookeeper(node)
    clear_zookeeper(node)
    start_zookeeper(node)


def restart_zookeeper(node):
    stop_zookeeper(node)
    start_zookeeper(node)


def clear_clickhouse_data(node):
    node.exec_in_container(
        [
            "bash",
            "-c",
            "rm -fr /var/lib/clickhouse/coordination/logs/* /var/lib/clickhouse/coordination/snapshots/*",
        ]
    )


def convert_zookeeper_data(node):
    node.exec_in_container(
        [
            "bash",
            "-c",
            "tar -cvzf /var/lib/clickhouse/zk-data.tar.gz /zookeeper/version-2",
        ]
    )
    cmd = "/usr/bin/clickhouse keeper-converter --zookeeper-logs-dir /zookeeper/version-2/ --zookeeper-snapshots-dir  /zookeeper/version-2/ --output-dir /var/lib/clickhouse/coordination/snapshots"
    node.exec_in_container(["bash", "-c", cmd])
    return os.path.join(
        "/var/lib/clickhouse/coordination/snapshots",
        node.exec_in_container(
            ["bash", "-c", "ls /var/lib/clickhouse/coordination/snapshots"]
        ).strip(),
    )


def stop_clickhouse(node):
    node.stop_clickhouse()


def start_clickhouse(node):
    node.start_clickhouse()
    keeper_utils.wait_until_connected(cluster, node)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_fake_zk(node, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(node.name) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def get_genuine_zk(node, timeout=30.0):
    CONNECTION_RETRIES = 100
    for i in range(CONNECTION_RETRIES):
        try:
            _genuine_zk_instance = KazooClient(
                hosts=cluster.get_instance_ip(node.name) + ":2181",
                timeout=timeout,
                connection_retry=KazooRetry(max_tries=20),
            )
            _genuine_zk_instance.start()
            return _genuine_zk_instance
        except KazooTimeoutError:
            if i == CONNECTION_RETRIES - 1:
                raise

            print(
                "Failed to connect to ZK cluster because of timeout. Restarting cluster and trying again."
            )
            time.sleep(0.2)
            restart_zookeeper(node)


def test_snapshot_and_load(started_cluster):
    genuine_connection = None
    fake_zks = []

    try:
        restart_and_clear_zookeeper(node1)
        genuine_connection = get_genuine_zk(node1)

        for node in [node1, node2, node3]:
            print("Stop and clear", node.name, "with dockerid", node.docker_id)
            stop_clickhouse(node)
            clear_clickhouse_data(node)

        for i in range(1000):
            genuine_connection.create("/test" + str(i), b"data")

        print("Data loaded to zookeeper")

        generate_zk_snapshot(node1)

        print("Data copied to node1")
        resulted_path = convert_zookeeper_data(node1)
        print("Resulted path", resulted_path)
        for node in [node2, node3]:
            print("Copy snapshot from", node1.name, "to", node.name)
            cluster.copy_file_from_container_to_container(
                node1, resulted_path, node, "/var/lib/clickhouse/coordination/snapshots"
            )

        print("Starting clickhouses")

        p = Pool(3)
        result = p.map_async(start_clickhouse, [node1, node2, node3])
        result.wait()

        print("Loading additional data")
        fake_zks = [get_fake_zk(node) for node in [node1, node2, node3]]
        for i in range(1000):
            fake_zk = random.choice(fake_zks)
            try:
                fake_zk.create("/test" + str(i + 1000), b"data")
            except Exception as ex:
                print("Got exception:" + str(ex))

        print("Final")
        fake_zks[0].create("/test10000", b"data")
    finally:
        for zk in fake_zks:
            if zk:
                zk.stop()
                zk.close()
        if genuine_connection:
            genuine_connection.stop()
            genuine_connection.close()
