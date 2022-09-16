#!/usr/bin/env python3
import time

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/s3.xml"], with_minio=True, with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/s3.xml"], with_minio=True, with_zookeeper=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/s3.xml"], with_minio=True, with_zookeeper=True
)

single_node_cluster = ClickHouseCluster(__file__)
small_node = single_node_cluster.add_instance(
    "small_node", main_configs=["configs/s3.xml"], with_minio=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_ttl_move_and_s3(started_cluster):
    for i, node in enumerate([node1, node2, node3]):
        node.query(
            """
            CREATE TABLE s3_test_with_ttl (date DateTime, id UInt32, value String)
            ENGINE=ReplicatedMergeTree('/clickhouse/tables/s3_test', '{}')
            ORDER BY id
            PARTITION BY id
            TTL date TO DISK 's3_disk'
            SETTINGS storage_policy='s3_and_default'
            """.format(
                i
            )
        )

    node1.query("SYSTEM STOP MOVES s3_test_with_ttl")

    node2.query("SYSTEM STOP MOVES s3_test_with_ttl")

    for i in range(30):
        if i % 2 == 0:
            node = node1
        else:
            node = node2

        node.query(
            f"INSERT INTO s3_test_with_ttl SELECT now() + 5, {i}, randomPrintableASCII(1048570)"
        )

    node1.query("SYSTEM SYNC REPLICA s3_test_with_ttl")
    node2.query("SYSTEM SYNC REPLICA s3_test_with_ttl")
    node3.query("SYSTEM SYNC REPLICA s3_test_with_ttl")

    assert node1.query("SELECT COUNT() FROM s3_test_with_ttl") == "30\n"
    assert node2.query("SELECT COUNT() FROM s3_test_with_ttl") == "30\n"

    node1.query("SYSTEM START MOVES s3_test_with_ttl")
    node2.query("SYSTEM START MOVES s3_test_with_ttl")

    assert node1.query("SELECT COUNT() FROM s3_test_with_ttl") == "30\n"
    assert node2.query("SELECT COUNT() FROM s3_test_with_ttl") == "30\n"

    for attempt in reversed(range(5)):
        time.sleep(5)

        print(
            node1.query(
                "SELECT * FROM system.parts WHERE table = 's3_test_with_ttl' FORMAT Vertical"
            )
        )

        minio = cluster.minio_client
        objects = minio.list_objects(cluster.minio_bucket, "data/", recursive=True)
        counter = 0
        for obj in objects:
            print(f"Objectname: {obj.object_name}, metadata: {obj.metadata}")
            counter += 1

        print(f"Total objects: {counter}")

        if counter == 300:
            break

        print(f"Attempts remaining: {attempt}")

    assert counter == 300


@pytest.fixture(scope="module")
def started_single_node_cluster():
    try:
        single_node_cluster.start()

        yield single_node_cluster
    finally:
        single_node_cluster.shutdown()


def test_move_and_s3_memory_usage(started_single_node_cluster):
    if small_node.is_built_with_sanitizer() or small_node.is_debug_build():
        pytest.skip("Disabled for debug and sanitizers. Too slow.")

    small_node.query(
        "CREATE TABLE s3_test_with_ttl (x UInt32, a String codec(NONE), b String codec(NONE), c String codec(NONE), d String codec(NONE), e String codec(NONE)) engine = MergeTree order by x partition by x SETTINGS storage_policy='s3_and_default'"
    )

    for _ in range(10):
        small_node.query(
            "insert into s3_test_with_ttl select 0, repeat('a', 100), repeat('b', 100), repeat('c', 100), repeat('d', 100), repeat('e', 100) from zeros(400000) settings max_block_size = 8192, max_insert_block_size=10000000, min_insert_block_size_rows=10000000"
        )

    # After this, we should have 5 columns per 10 * 100 * 400000 ~ 400 MB; total ~2G data in partition
    small_node.query("optimize table s3_test_with_ttl final")

    small_node.query("system flush logs")
    # Will take memory usage from metric_log.
    # It is easier then specifying total memory limit (insert queries can hit this limit).
    small_node.query("truncate table system.metric_log")

    small_node.query(
        "alter table s3_test_with_ttl move partition 0 to volume 'external'",
        settings={"send_logs_level": "error"},
    )
    small_node.query("system flush logs")
    max_usage = small_node.query(
        "select max(CurrentMetric_MemoryTracking) from system.metric_log"
    )
    # 3G limit is a big one. However, we can hit it anyway with parallel s3 writes enabled.
    # Also actual value can be bigger because of memory drift.
    # Increase it a little bit if test fails.
    assert int(max_usage) < 3e9
    res = small_node.query(
        "select * from system.errors where last_error_message like '%Memory limit%' limit 1"
    )
    assert res == ""
