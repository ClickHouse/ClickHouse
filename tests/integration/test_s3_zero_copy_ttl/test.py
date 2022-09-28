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
