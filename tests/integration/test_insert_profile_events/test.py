import logging
from time import sleep

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/config.xml"],
    with_zookeeper=True,
    # Server is very short on memory and may fail while initializing remote database disk
    with_remote_database_disk=False,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_insert_profile_events(started_cluster):
    node = started_cluster.instances["node"]
    select_insert_query = (
        "SELECT value FROM system.events WHERE event = 'InsertQuery'"
    )
    select_async_insert_query = (
        "SELECT value FROM system.events WHERE event = 'AsyncInsertQuery'"
    )
   
    non_async_inserts = 2   
    async_inserts = 2    
    inserts = non_async_inserts  + async_inserts 
    
    node.query(
        "CREATE TABLE test_insert_profile_events  (id UInt32, s String) ENGINE = MergeTree"
    )

    node.query(
        "INSERT INTO test_insert_profile_events VALUES (1,'Non Async Insert')"
    )
    node.query(
        "INSERT INTO test_insert_profile_events VALUES (2,'Non Async Insert')"
    )

    node.query(
        "INSERT INTO test_insert_profile_events SETTINGS async_insert = 1 VALUES (3,'Async Insert')"
    )
    node.query(
        "INSERT INTO test_insert_profile_events SETTINGS async_insert = 1 VALUES (4,'Async Insert')"
    )
    
    insert_count = node.query(select_insert_query).strip()
    async_insert_count = node.query(select_async_insert_query).strip()
    
    current_insert_count = int(insert_count) if insert_count else 0
    current_async_insert_count = int(async_insert_count) if async_insert_count else 0
 
    assert current_insert_count == inserts
    assert current_async_insert_count == async_inserts

    node.query("DROP TABLE IF EXISTS test_insert_profile_events SYNC")

