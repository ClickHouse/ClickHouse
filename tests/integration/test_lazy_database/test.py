import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["configs/storage_policy.xml"],
            with_minio=True,
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


def list_of_files_on_ch_disk(node, disk, path):
    disk_path = node.query(
        f"SELECT path FROM system.disks WHERE name='{disk}'"
    ).splitlines()[0]
    return node.exec_in_container(
        ["bash", "-c", f"ls {os.path.join(disk_path, path)}"], user="root"
    )


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param("Log"),
    ],
)
@pytest.mark.parametrize(
    "disk,check_s3",
    [
        pytest.param("default", False),
        pytest.param("s3", True),
    ],
)
@pytest.mark.parametrize(
    "delay",
    [
        pytest.param(0),
        pytest.param(4),
    ],
)
def test_drop_table(cluster, engine, disk, check_s3, delay):
    node = cluster.instances["node"]

    node.query("DROP DATABASE IF EXISTS lazy")
    node.query("CREATE DATABASE lazy ENGINE=Lazy(2)")
    node.query(
        "CREATE TABLE lazy.table (id UInt64) ENGINE={} SETTINGS disk = '{}'".format(
            engine,
            disk,
        )
    )

    node.query("INSERT INTO lazy.table SELECT number FROM numbers(10)")
    assert node.query("SELECT count(*) FROM lazy.table") == "10\n"
    if delay:
        time.sleep(delay)
    node.query("DROP TABLE lazy.table SYNC")

    if check_s3:
        # There mustn't be any orphaned data
        assert_objects_count(cluster, 0)

    # Local data must be removed
    assert list_of_files_on_ch_disk(node, disk, "data/lazy/") == ""
