#!/usr/bin/env python3

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_logs_contain_with_retry
import os

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True, stay_alive=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        for i, node in enumerate((node1,)):
            node_name = "node" + str(i + 1)
            node.query(
                """
                CREATE TABLE test_table(date Date, id UInt32, dummy UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_table', '{}')
                PARTITION BY date ORDER BY id
                """.format(
                    node_name
                )
            )

        yield cluster

    finally:
        cluster.shutdown()


def test_old_dirs_cleanup(start_cluster):
    node1.query("INSERT INTO test_table VALUES (toDate('2020-01-01'), 1, 10)")
    assert node1.query("SELECT count() FROM test_table") == "1\n"

    node1.stop_clickhouse()

    node1.exec_in_container(
        [
            "bash",
            "-c",
            "mv /var/lib/clickhouse/data/default/test_table/20200101_0_0_0 /var/lib/clickhouse/data/default/test_table/delete_tmp_20200101_0_0_0",
        ],
        privileged=True,
    )

    node1.start_clickhouse()

    result = node1.exec_in_container(
        ["bash", "-c", "ls /var/lib/clickhouse/data/default/test_table/"],
        privileged=True,
    )

    # Replaced empty part
    assert "20200101_0_0_0" in result
    assert node1.query("SELECT count() FROM test_table") == "0\n"

    assert_logs_contain_with_retry(node1, "Removing temporary directory")
    assert_logs_contain_with_retry(node1, "delete_tmp_20200101_0_0_0")
