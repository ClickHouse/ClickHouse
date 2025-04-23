import logging
import os
import random
import string

import pytest

from helpers.cluster import ClickHouseCluster, QueryRuntimeException


LARGE_TABLE_NAME_LENGTH = 220


cluster = ClickHouseCluster(__file__)
old_node = cluster.add_instance(
    "old_node",
    image="clickhouse/clickhouse-server",
    tag="24.9.2.42",
    with_zookeeper=True,
    with_installed_binary=True,
)
new_node = cluster.add_instance(
    "new_node",
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def generate_name(length):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


def test_backward_compatibility(start_cluster):
    """
    New node has table name length check, but recoverLostReplica should still work without any issues
    because we only perform the check for initial create queries and skip it for secondary ones.
    """

    table_name = generate_name(LARGE_TABLE_NAME_LENGTH)

    # create database
    old_node.query("DROP DATABASE IF EXISTS rdb SYNC")
    old_node.query("CREATE DATABASE rdb ENGINE = Replicated('/test/rdb', 'shard1', 'replica' || '1')")

    # create table with long name
    old_node.query(f"DROP TABLE IF EXISTS rdb.{table_name}")
    old_node.query(f"CREATE TABLE rdb.{table_name} (col String) Engine=MergeTree ORDER BY tuple()")

    # recreate the database on the new replica, so it runs recoverLostReplica
    new_node.query("DROP DATABASE IF EXISTS rdb SYNC")
    new_node.query("CREATE DATABASE rdb ENGINE = Replicated('/test/rdb', 'shard1', 'replica' || '2')")
    new_node.query("SYSTEM SYNC DATABASE REPLICA rdb")


def test_check_table_name_length(start_cluster):
    """
    Verify that the new node gets error trying to create a table with name that is too long.
    """

    table_name = generate_name(LARGE_TABLE_NAME_LENGTH)

    # create database
    new_node.query("DROP DATABASE IF EXISTS rdb2 SYNC")
    new_node.query(
        "CREATE DATABASE rdb2 ENGINE = Replicated('/test/rdb2', 'shard1', 'replica' || '1');"
    )

    # try to create table with long name
    new_node.query(f"DROP TABLE IF EXISTS rdb2.{table_name}")
    with pytest.raises(QueryRuntimeException, match="ARGUMENT_OUT_OF_BOUND"):
        new_node.query(f"CREATE TABLE rdb2.{table_name} (col String) Engine=MergeTree ORDER BY tuple()")
