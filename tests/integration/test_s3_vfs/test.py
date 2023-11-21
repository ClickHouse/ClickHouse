import datetime
import logging
import time

import pytest
from helpers.cluster import ClickHouseCluster
import helpers.keeper_utils as ku

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.d/s3.xml"],
    macros={"replica": "1"},
    with_minio=True,
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/s3.xml"],
    macros={"replica": "2"},
    with_minio=True,
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# TODO should possibly rewrite to KeeperClient
def get_large_objects_count(cluster, size=100, folder="data"):
    minio = cluster.minio_client
    counter = 0
    for obj in minio.list_objects(
        cluster.minio_bucket, "{}/".format(folder), recursive=True
    ):
        if obj.size is not None and obj.size >= size:
            counter = counter + 1
    return counter


def check_objects_exist(cluster, object_list, folder="data"):
    minio = cluster.minio_client
    for obj in object_list:
        if obj:
            minio.stat_object(cluster.minio_bucket, "{}/{}".format(folder, obj))


def check_objects_not_exisis(cluster, object_list, folder="data"):
    minio = cluster.minio_client
    for obj in object_list:
        if obj:
            try:
                minio.stat_object(cluster.minio_bucket, "{}/{}".format(folder, obj))
            except Exception as error:
                assert "NoSuchKey" in str(error)
            else:
                assert False, "Object {} should not be exists".format(obj)


def wait_for_large_objects_count(cluster, expected, size=100, timeout=30):
    while timeout > 0:
        if get_large_objects_count(cluster, size=size) == expected:
            return
        timeout -= 1
        time.sleep(1)
    assert get_large_objects_count(cluster, size=size) == expected


def wait_for_active_parts(node, num_expected_parts, table_name, timeout=30):
    deadline = time.monotonic() + timeout
    num_parts = 0
    while time.monotonic() < deadline:
        num_parts_str = node.query(
            "select count() from system.parts where table = '{}' and active".format(
                table_name
            )
        )
        num_parts = int(num_parts_str.strip())
        if num_parts == num_expected_parts:
            return

        time.sleep(0.2)

    assert num_parts == num_expected_parts


# Result of `get_large_objects_count` can be changed in other tests, so run this case at the beginning
@pytest.mark.order(0)
@pytest.mark.parametrize("policy", ["s3"])
def test_s3_vfs(started_cluster, policy):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(
        """
        CREATE TABLE s3_test ON CLUSTER test_cluster (id UInt32, value String)
        ENGINE=ReplicatedMergeTree('/clickhouse/tables/s3_test', '{}')
        ORDER BY id
        SETTINGS storage_policy='{}'
        """.format(
            "{replica}", policy
        )
    )

    node1.query("INSERT INTO s3_test VALUES (0,'data'),(1,'data')")
    node2.query("SYSTEM SYNC REPLICA s3_test", timeout=30)
    assert (
        node1.query("SELECT * FROM s3_test order by id FORMAT Values")
        == "(0,'data'),(1,'data')"
    )
    assert (
        node2.query("SELECT * FROM s3_test order by id FORMAT Values")
        == "(0,'data'),(1,'data')"
    )

    # Based on version 21.x - should be only 1 file with size 100+ (checksums.txt), used by both nodes
    assert get_large_objects_count(cluster) == 1

    node2.query("INSERT INTO s3_test VALUES (2,'data'),(3,'data')")
    node1.query("SYSTEM SYNC REPLICA s3_test", timeout=30)

    assert (
        node2.query("SELECT * FROM s3_test order by id FORMAT Values")
        == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"
    )
    assert (
        node1.query("SELECT * FROM s3_test order by id FORMAT Values")
        == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"
    )

    # Based on version 21.x - two parts
    wait_for_large_objects_count(cluster, 2)

    time.sleep(11)  # wait for GC to start but not remove anything

    node1.query("DROP TABLE s3_test ON CLUSTER test_cluster SYNC")

    time.sleep(10)  # wait for GC to start and clean data

    # Merges don't work as for now
    #     node1.query("OPTIMIZE TABLE s3_test FINAL")
    #
    #     # Based on version 21.x - after merge, two old parts and one merged
    #     wait_for_large_objects_count(cluster, 3)
    #
    #     # Based on version 21.x - after cleanup - only one merged part
    #     wait_for_large_objects_count(cluster, 1, timeout=60)
    #
    #     node1.query("DROP TABLE IF EXISTS s3_test SYNC")
    #     node2.query("DROP TABLE IF EXISTS s3_test SYNC")
    assert False
