#!/usr/bin/env python3

import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_logs_contain_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True, stay_alive=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_old_dirs_cleanup(start_cluster):
    node1.query("DROP TABLE IF EXISTS test_table SYNC")
    node1.query(
        """
        CREATE TABLE test_table(date Date, id UInt32, dummy UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_table', 'node1')
        PARTITION BY date ORDER BY id
        SETTINGS cleanup_delay_period=3600, max_cleanup_delay_period=3600
        """
    )

    node1.query("INSERT INTO test_table VALUES (toDate('2020-01-01'), 1, 10)")
    assert node1.query("SELECT count() FROM test_table") == "1\n"

    data_path = node1.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='test_table'"
    ).strip()

    node1.stop_clickhouse()

    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"mv {data_path}/20200101_0_0_0 {data_path}/delete_tmp_20200101_0_0_0",
        ],
        privileged=True,
    )

    node1.start_clickhouse()

    assert_logs_contain_with_retry(node1, "Removing temporary directory .*delete_tmp_20200101_0_0_0")

    assert_logs_contain_with_retry(node1, "Created empty part 20200101_0_0_0 instead of lost part")
    # Replaced empty part
    result = node1.exec_in_container(
        ["bash", "-c", f"ls {data_path}/"],
        privileged=True,
    )
    assert "20200101_0_0_0" in result
    assert node1.query("SELECT count() FROM test_table") == "0\n"

    node1.query("DROP TABLE test_table SYNC")
