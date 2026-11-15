import logging
from time import sleep

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/config.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_insert_query_profile_events(started_cluster):
    select_insert_query = (
        "SELECT value FROM system.events WHERE event = 'InsertQuery'"
    )
    select_async_insert_query = (
        "SELECT value FROM system.events WHERE event = 'AsyncInsertQuery'"
    )

    non_async_inserts = 2
    async_inserts = 2
    inserts = non_async_inserts + async_inserts

    insert_count = node.query(select_insert_query).strip()
    async_insert_count = node.query(select_async_insert_query).strip()

    orig_insert_count = int(insert_count) if insert_count else 0
    orig_async_insert_count = int(async_insert_count) if async_insert_count else 0

    node.query(
        "DROP TABLE IF EXISTS test_insert_query_profile_events"
    )

    try:
        node.query(
            "CREATE TABLE test_insert_query_profile_events  (id UInt32, s String) ENGINE = Memory"
        )

        node.query(
            "INSERT INTO test_insert_query_profile_events SETTINGS async_insert=0 VALUES (1,'Non Async Insert')"
        )
        node.query(
            "INSERT INTO test_insert_query_profile_events SETTINGS async_insert=0 VALUES (2,'Non Async Insert')"
        )

        node.query(
            "INSERT INTO test_insert_query_profile_events SETTINGS async_insert=1 VALUES (3,'Async Insert')"
        )
        node.query(
            "INSERT INTO test_insert_query_profile_events SETTINGS async_insert=1 VALUES (4,'Async Insert')"
        )

        insert_count = node.query(select_insert_query).strip()
        async_insert_count = node.query(select_async_insert_query).strip()

        current_insert_count = int(insert_count) if insert_count else 0
        current_async_insert_count = int(async_insert_count) if async_insert_count else 0

        assert current_insert_count - orig_insert_count == inserts
        assert current_async_insert_count - orig_async_insert_count == async_inserts
    finally:
        node.query("DROP TABLE IF EXISTS test_insert_query_profile_events SYNC")
