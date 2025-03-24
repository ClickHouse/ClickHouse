#!/usr/bin/env python3

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_data_files_for_table(node, table_name):
    raw_output = node.exec_in_container(
        ["bash", "-c", "ls /var/lib/clickhouse/data/default/{}".format(table_name)]
    )
    return raw_output.strip().split("\n")


def test_empty_parts_optimize(start_cluster):
    for n, node in enumerate([node1, node2]):
        node.query(
            """
            CREATE TABLE empty (key UInt32, val UInt32, date Datetime)
            ENGINE=ReplicatedSummingMergeTree('/clickhouse/01560_optimize_on_insert', '{}', val)
            PARTITION BY date ORDER BY key;
        """.format(
                n + 1
            )
        )

    node1.query(
        "INSERT INTO empty VALUES (1, 1, '2020-01-01'), (1, 1, '2020-01-01'), (1, -2, '2020-01-01')"
    )

    node2.query("SYSTEM SYNC REPLICA empty", timeout=15)

    assert node1.query("SELECT * FROM empty") == ""
    assert node2.query("SELECT * FROM empty") == ""

    # No other tmp files exists
    assert set(get_data_files_for_table(node1, "empty")) == {
        "detached",
        "format_version.txt",
    }
    assert set(get_data_files_for_table(node2, "empty")) == {
        "detached",
        "format_version.txt",
    }

    node1.query(
        "INSERT INTO empty VALUES (1, 1, '2020-02-01'), (1, 1, '2020-02-01'), (1, -2, '2020-02-01')",
        settings={"insert_quorum": 2},
    )

    assert node1.query("SELECT * FROM empty") == ""
    assert node2.query("SELECT * FROM empty") == ""
