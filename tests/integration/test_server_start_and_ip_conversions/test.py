#!/usr/bin/env python3
import logging

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_restart_success_ipv4():
    node.query(
        """
        CREATE TABLE ipv4_test
        (
            id UInt64,
            value String
        ) ENGINE=MergeTree ORDER BY id""",
        settings={"cast_ipv4_ipv6_default_on_conversion_error": 1},
    )

    node.query(
        "ALTER TABLE ipv4_test MODIFY COLUMN value IPv4 DEFAULT ''",
        settings={"cast_ipv4_ipv6_default_on_conversion_error": 1},
    )

    node.restart_clickhouse()

    assert node.query("SELECT 1") == "1\n"


def test_restart_success_ipv6():
    node.query(
        """
        CREATE TABLE ipv6_test
        (
            id UInt64,
            value String
        ) ENGINE=MergeTree ORDER BY id""",
        settings={"cast_ipv4_ipv6_default_on_conversion_error": 1},
    )

    node.query(
        "ALTER TABLE ipv6_test MODIFY COLUMN value IPv6 DEFAULT ''",
        settings={"cast_ipv4_ipv6_default_on_conversion_error": 1},
    )

    node.restart_clickhouse()

    assert node.query("SELECT 1") == "1\n"
