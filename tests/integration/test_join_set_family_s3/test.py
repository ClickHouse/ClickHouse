import logging
import sys

import pytest

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["configs/minio.xml", "configs/ssl.xml"],
            with_minio=True,
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()

        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def assert_objects_count(cluster, objects_count, path="data/"):
    minio = cluster.minio_client
    s3_objects = list(minio.list_objects(cluster.minio_bucket, path, recursive=True))
    if objects_count != len(s3_objects):
        for s3_object in s3_objects:
            object_meta = minio.stat_object(cluster.minio_bucket, s3_object.object_name)
            logging.info("Existing S3 object: %s", str(object_meta))
        assert objects_count == len(s3_objects)


def test_set_s3(cluster):
    node = cluster.instances["node"]

    node.query("CREATE TABLE testLocalSet (n UInt64) Engine = Set")
    node.query("CREATE TABLE testS3Set (n UInt64) Engine = Set SETTINGS disk='s3'")

    node.query("INSERT INTO TABLE testLocalSet VALUES (1)")
    node.query("INSERT INTO TABLE testS3Set VALUES (1)")

    assert (
        node.query(
            "SELECT number in testLocalSet, number in testS3Set FROM system.numbers LIMIT 3"
        )
        == "0\t0\n1\t1\n0\t0\n"
    )
    assert_objects_count(cluster, 1)

    node.query("INSERT INTO TABLE testLocalSet VALUES (2)")
    node.query("INSERT INTO TABLE testS3Set VALUES (2)")

    assert (
        node.query(
            "SELECT number in testLocalSet, number in testS3Set FROM system.numbers LIMIT 3"
        )
        == "0\t0\n1\t1\n1\t1\n"
    )
    assert_objects_count(cluster, 2)

    node.restart_clickhouse()
    assert (
        node.query(
            "SELECT number in testLocalSet, number in testS3Set FROM system.numbers LIMIT 3"
        )
        == "0\t0\n1\t1\n1\t1\n"
    )

    node.query("TRUNCATE TABLE testLocalSet")
    node.query("TRUNCATE TABLE testS3Set")

    assert (
        node.query(
            "SELECT number in testLocalSet, number in testS3Set FROM system.numbers LIMIT 3"
        )
        == "0\t0\n0\t0\n0\t0\n"
    )
    assert_objects_count(cluster, 0)

    node.query("DROP TABLE testLocalSet")
    node.query("DROP TABLE testS3Set")


def test_join_s3(cluster):
    node = cluster.instances["node"]

    node.query(
        "CREATE TABLE testLocalJoin(`id` UInt64, `val` String) ENGINE = Join(ANY, LEFT, id)"
    )
    node.query(
        "CREATE TABLE testS3Join(`id` UInt64, `val` String) ENGINE = Join(ANY, LEFT, id) SETTINGS disk='s3', join_any_take_last_row = 1"
    )

    node.query("INSERT INTO testLocalJoin VALUES (1, 'a')")
    for i in range(1, 10):
        c = chr(ord("a") + i)
        node.query(f"INSERT INTO testLocalJoin VALUES (1, '{c}')")

    # because of `join_any_take_last_row = 1` we expect the last row with 'a' value
    for i in range(1, 10):
        c = chr(ord("a") + i)
        node.query(f"INSERT INTO testS3Join VALUES (1, '{c}')")
    node.query("INSERT INTO testS3Join VALUES (1, 'a')")

    assert (
        node.query(
            "SELECT joinGet('testLocalJoin', 'val', number) as local, joinGet('testS3Join', 'val', number) as s3 FROM system.numbers LIMIT 3"
        )
        == "\t\na\ta\n\t\n"
    )
    assert_objects_count(cluster, 10)

    node.query("INSERT INTO testLocalJoin VALUES (2, 'b')")
    node.query("INSERT INTO testS3Join VALUES (2, 'b')")

    assert (
        node.query(
            "SELECT joinGet('testLocalJoin', 'val', number) as local, joinGet('testS3Join', 'val', number) as s3 FROM system.numbers LIMIT 3"
        )
        == "\t\na\ta\nb\tb\n"
    )
    assert_objects_count(cluster, 11)

    node.restart_clickhouse()
    assert (
        node.query(
            "SELECT joinGet('testLocalJoin', 'val', number) as local, joinGet('testS3Join', 'val', number) as s3 FROM system.numbers LIMIT 3"
        )
        == "\t\na\ta\nb\tb\n"
    )

    node.query("TRUNCATE TABLE testLocalJoin")
    node.query("TRUNCATE TABLE testS3Join")

    assert (
        node.query(
            "SELECT joinGet('testLocalJoin', 'val', number) as local, joinGet('testS3Join', 'val', number) as s3 FROM system.numbers LIMIT 3"
        )
        == "\t\n\t\n\t\n"
    )
    assert_objects_count(cluster, 0)

    node.query("DROP TABLE testLocalJoin")
    node.query("DROP TABLE testS3Join")
