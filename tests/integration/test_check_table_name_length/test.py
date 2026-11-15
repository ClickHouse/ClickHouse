import logging
import os
import random
import string
import time

import pytest

from helpers.cluster import ClickHouseCluster, QueryRuntimeException

DATABASE_NAME_LENGTH = 5
LARGE_TABLE_NAME_LENGTH = 211


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


def generate_random_name(length):
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))


def test_backward_compatibility(start_cluster):
    """
    New node has table name length check, but recoverLostReplica should still work without any issues
    because we only perform the check for initial create queries and skip it for secondary ones.
    """
    db_name = generate_random_name(DATABASE_NAME_LENGTH)
    table_name = generate_random_name(LARGE_TABLE_NAME_LENGTH)

    # create database
    old_node.query(f"CREATE DATABASE {db_name} ENGINE = Replicated('/test/{db_name}', 'shard1', 'replica' || '1')")

    # create table with long name
    old_node.query(f"CREATE TABLE {db_name}.{table_name} (col String) Engine=MergeTree ORDER BY tuple()")

    # the table should be successfully created on new_node via recoverLostReplica
    new_node.query(f"CREATE DATABASE {db_name} ENGINE = Replicated('/test/{db_name}', 'shard1', 'replica' || '2')")
    new_node.query(f"SYSTEM SYNC DATABASE REPLICA {db_name}")


def test_check_table_name_length(start_cluster):
    """
    Verify that the new node gets error trying to create a table with name that is too long.
    """

    db_name = generate_random_name(DATABASE_NAME_LENGTH)
    table_name = generate_random_name(LARGE_TABLE_NAME_LENGTH)

    # create database
    new_node.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")
    new_node.query(
        f"CREATE DATABASE {db_name} ENGINE = Replicated('/test/{db_name}', 'shard1', 'replica' || '1');"
    )

    # try to create table with long name
    new_node.query(f"DROP TABLE IF EXISTS {db_name}.{table_name}")
    with pytest.raises(QueryRuntimeException, match="ARGUMENT_OUT_OF_BOUND"):
        new_node.query(f"CREATE TABLE {db_name}.{table_name} (col String) Engine=MergeTree ORDER BY tuple()")
