"""
This test is similar to test_cross_replication, except
in this test we write into per-node tables and read from the distributed table.

The default database in the distributed table definition is left empty on purpose to test
default database deduction.
"""

from contextlib import contextmanager

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV


def bootstrap(cluster):
    for i, node in enumerate(list(cluster.instances.values())):
        node.query(f"CREATE DATABASE IF NOT EXISTS r{i}")
        node.query(f"CREATE TABLE r{i}.test_data(v UInt64) ENGINE = Memory()")
        node.query(f"INSERT INTO r{i}.test_data SELECT * FROM numbers(10)")
        node.query(
            f"""CREATE TABLE default.test AS r{i}.test_data ENGINE = Distributed(secure, '', test_data, rand())"""
        )


@contextmanager
def start_cluster():
    cluster_disabled = ClickHouseCluster(__file__)
    cluster_disabled.add_instance(
        "node1",
        main_configs=["configs/remote_servers.xml"],
        user_configs=["configs/users.xml"],
    )
    cluster_disabled.add_instance(
        "node2",
        main_configs=["configs/remote_servers.xml"],
        user_configs=["configs/users.xml"],
    )
    try:
        cluster_disabled.start()
        bootstrap(cluster_disabled)
        yield cluster_disabled
    finally:
        cluster_disabled.shutdown()


def test_query():
    with start_cluster() as cluster:
        node1 = cluster.instances["node1"]
        assert TSV(node1.query("SELECT count() FROM default.test")) == TSV("20")
