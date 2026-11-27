import os
import sys
import time
import uuid
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.d/settings.xml"],
    user_configs=["configs/users.d/query_log.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/settings.xml"],
    user_configs=["configs/users.d/query_log.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 2},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_waiting_replicated_database_hosts(started_cluster):
    node1.query("DROP DATABASE IF EXISTS db SYNC")
    node2.query("DROP DATABASE IF EXISTS  db SYNC")

    node1.query("DROP TABLE IF EXISTS t SYNC")
    node2.query("DROP TABLE IF EXISTS t SYNC")

    node1.query(
        "CREATE DATABASE db ENGINE=Replicated('/test/db', '{shard}', '{replica}')"
    )
    node2.query(
        "CREATE DATABASE db ENGINE=Replicated('/test/db', '{shard}', '{replica}')"
    )

    query_id = str(uuid.uuid4())
    node1.query(
        "CREATE TABLE t ON CLUSTER 'db' (x INT, y INT) ENGINE=MergeTree ORDER BY x",
        settings={"distributed_ddl_output_mode": "throw_only_active"},
        query_id=query_id,
    )
    assert (
        node2.query("SELECT count() FROM system.tables WHERE name='t'").strip() == "1"
    )
    node1.query("SYSTEM FLUSH LOGS")
    assert (
        node1.query(
            f"SELECT count() FROM system.text_log WHERE query_id='{query_id}' AND level='Warning' AND message LIKE '%Did not find active hosts%'"
        ).strip()
        == "0"
    )

    node1.query("DROP DATABASE IF EXISTS db SYNC")
    node2.query("DROP DATABASE IF EXISTS  db SYNC")

    node1.query("DROP TABLE IF EXISTS t SYNC")
    node2.query("DROP TABLE IF EXISTS t SYNC")
