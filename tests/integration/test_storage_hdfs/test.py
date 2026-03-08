#!/usr/bin/env python3

import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_hive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _wait_hdfs_ready():
    probe_path = f"/tmp/ch_hdfs_probe_{uuid.uuid4().hex}.parquet"
    probe_uri = f"hdfs://hivetest:9000{probe_path}"

    last_error = ""
    for _ in range(40):
        last_error = node.query_and_get_error(
            f"SELECT count() FROM hdfs('{probe_uri}', 'Parquet', 'x UInt8')"
        )
        if "compiled without USE_HDFS" in last_error:
            pytest.skip("ClickHouse is compiled without HDFS support")

        if "Connection refused" not in last_error and "Unable to connect to HDFS" not in last_error:
            return

        time.sleep(1)

    pytest.fail(f"HDFS is not ready after waiting. Last error: {last_error}")


def test_hdfs_parquet_large_read_path(started_cluster):
    _wait_hdfs_ready()

    file_path = f"/tmp/ch_86515_{uuid.uuid4().hex}.parquet"
    hdfs_uri = f"hdfs://hivetest:9000{file_path}"

    node.query(
        f"""
        INSERT INTO FUNCTION hdfs('{hdfs_uri}', 'Parquet', 'id UInt64, payload String')
        SELECT
            number,
            repeat('x', 1024)
        FROM numbers(250000)
        SETTINGS hdfs_truncate_on_insert = 1
        """
    )

    query = f"""
        SELECT count()
        FROM hdfs('{hdfs_uri}', 'Parquet', 'id UInt64, payload String')
        SETTINGS max_read_buffer_size_remote_fs = 100000000
    """
    assert node.query(query).strip() == "250000"
