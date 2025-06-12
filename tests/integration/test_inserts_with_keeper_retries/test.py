#!/usr/bin/env python3

import threading
import time
import uuid
from multiprocessing.dummy import Pool

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/storage_conf.xml"],
    with_zookeeper=True,
    with_minio=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "engine,storage_policy",
    [
        ("ReplicatedMergeTree", "default"),
    ],
)
def test_replica_inserts_with_keeper_restart(started_cluster, engine, storage_policy):
    try:
        node1.query(
            f"CREATE TABLE r (a UInt64, b String) ENGINE={engine}('/test/r', '0') ORDER BY tuple() SETTINGS storage_policy='{storage_policy}'"
        )

        p = Pool(1)
        zk_stopped_event = threading.Event()

        def zoo_restart(zk_stopped_event):
            cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
            zk_stopped_event.set()
            cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])

        job = p.apply_async(zoo_restart, (zk_stopped_event,))

        zk_stopped_event.wait(90)

        node1.query(
            "INSERT INTO r SELECT number, toString(number) FROM numbers(10) SETTINGS insert_keeper_max_retries=20"
        )
        node1.query(
            "INSERT INTO r SELECT number, toString(number) FROM numbers(10, 10) SETTINGS insert_keeper_max_retries=20"
        )

        job.wait()
        p.close()
        p.join()

        assert node1.query("SELECT COUNT() FROM r") == "20\n"

    finally:
        node1.query("DROP TABLE IF EXISTS r SYNC")


@pytest.mark.parametrize(
    "engine,storage_policy",
    [
        ("ReplicatedMergeTree", "default"),
    ],
)
def test_replica_inserts_with_keeper_disconnect(
    started_cluster, engine, storage_policy
):
    try:
        node1.query(
            f"CREATE TABLE r2 (a UInt64, b String) ENGINE={engine}('/test/r2', '0') ORDER BY tuple() SETTINGS storage_policy='{storage_policy}'"
        )

        p = Pool(1)
        disconnect_event = threading.Event()

        def keeper_disconnect(node, event):
            with PartitionManager() as pm:
                pm.drop_instance_zk_connections(node)
                event.set()

        job = p.apply_async(
            keeper_disconnect,
            (
                node1,
                disconnect_event,
            ),
        )
        disconnect_event.wait(90)

        node1.query(
            "INSERT INTO r2 SELECT number, toString(number) FROM numbers(10) SETTINGS insert_keeper_max_retries=20"
        )
        node1.query(
            "INSERT INTO r2 SELECT number, toString(number) FROM numbers(10, 10) SETTINGS insert_keeper_max_retries=20"
        )

        job.wait()
        p.close()
        p.join()

        assert node1.query("SELECT COUNT() FROM r2") == "20\n"

    finally:
        node1.query("DROP TABLE IF EXISTS r2 SYNC")


@pytest.mark.parametrize(
    "engine,storage_policy",
    [
        ("ReplicatedMergeTree", "default"),
    ],
)
def test_query_timeout_with_zk_down(started_cluster, engine, storage_policy):
    try:
        node1.query(
            f"CREATE TABLE zk_down (a UInt64, b String) ENGINE={engine}('/test/zk_down', '0') ORDER BY tuple() SETTINGS storage_policy='{storage_policy}'"
        )

        cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])

        start_time = time.time()
        with pytest.raises(QueryRuntimeException):
            node1.query(
                "INSERT INTO zk_down SELECT number, toString(number) FROM numbers(10) SETTINGS insert_keeper_max_retries=10000, insert_keeper_retry_max_backoff_ms=1000, max_execution_time=1"
            )
        finish_time = time.time()
        assert finish_time - start_time < 10
    finally:
        cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
        node1.query("DROP TABLE IF EXISTS zk_down SYNC")


@pytest.mark.parametrize(
    "engine,storage_policy",
    [
        ("ReplicatedMergeTree", "default"),
    ],
)
def test_retries_should_not_wait_for_global_connection(
    started_cluster, engine, storage_policy
):
    pm = PartitionManager()
    try:
        node1.query(
            f"CREATE TABLE zk_down_retries (a UInt64, b String) ENGINE={engine}('/test/zk_down', '0') ORDER BY tuple() SETTINGS storage_policy='{storage_policy}'"
        )

        cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
        # Apart from stopping keepers, we introduce a network delay to make connection retries slower
        # We want to check that retries are not blocked during that time
        pm.add_network_delay(node1, 1000)

        query_id = uuid.uuid4()

        with pytest.raises(QueryRuntimeException):
            node1.query(
                "INSERT INTO zk_down_retries SELECT number, toString(number) FROM numbers(10) SETTINGS insert_keeper_max_retries=10, insert_keeper_retry_max_backoff_ms=100",
                query_id=str(query_id),
            )
        pm.heal_all()
        # Use query_log for execution time since we want to ignore the network delay introduced (also in client)
        node1.query("SYSTEM FLUSH LOGS")
        res = node1.query(
            f"SELECT query_duration_ms FROM system.query_log WHERE type != 'QueryStart' AND query_id = '{query_id}'"
        )
        query_duration = int(res)
        # It should be around 1 second. 5 seconds is being generous (debug and so on). Used to take 35 seconds without the fix
        assert query_duration < 5000
    finally:
        pm.heal_all()
        cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
        node1.query("DROP TABLE IF EXISTS zk_down_retries SYNC")
