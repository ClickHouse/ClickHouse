import pytest
import random

import helpers.cluster
from helpers.test_tools import TSV

cluster = helpers.cluster.ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
)

node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize("db_engine", ["Replicated"])
@pytest.mark.parametrize("table_engine", ["ReplicatedMergeTree"])
def test_lwu_replicated_database(started_cluster, db_engine, table_engine):
    db_name = "lwu_db" + str(random.randint(0, 10000000))
    settings = {
        "allow_experimental_lightweight_update": 1,
        "lightweight_delete_mode": "lightweight_update_force",
    }

    node1.query(f"DROP DATABASE IF EXISTS {db_name}")
    node2.query(f"DROP DATABASE IF EXISTS {db_name}")

    if db_engine == "Replicated":
        node1.query(
            f"CREATE DATABASE {db_name} ENGINE = Replicated('/test/{db_name}', 'shard1', 'r1')"
        )
        node2.query(
            f"CREATE DATABASE {db_name} ENGINE = Replicated('/test/{db_name}', 'shard1', 'r2')"
        )
    else:
        node1.query(f"CREATE DATABASE {db_name} ENGINE = {db_engine}")

    node1.query(
        f"""
        CREATE TABLE {db_name}.lwu_table (x Int32, y String) ENGINE = {table_engine} ORDER BY x
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"""
    )

    node1.query(f"INSERT INTO {db_name}.lwu_table VALUES (1, 'a'), (2, 'b') (3, 'c')")

    node1.query(f"DELETE FROM {db_name}.lwu_table WHERE x = 2", settings=settings)
    node1.query(
        f"UPDATE {db_name}.lwu_table SET y = 'updated' WHERE x = 1", settings=settings
    )

    if db_engine == "Replicated":
        node2.query(f"SYSTEM SYNC DATABASE REPLICA {db_name}")

    node2.query(f"SYSTEM SYNC REPLICA {db_name}.lwu_table")

    expected = "1\tupdated\n3\tc\n"

    assert TSV(node1.query(f"SELECT * FROM {db_name}.lwu_table")) == TSV(expected)
    assert TSV(node2.query(f"SELECT * FROM {db_name}.lwu_table")) == TSV(expected)

    node1.query("SYSTEM FLUSH LOGS")
    node2.query("SYSTEM FLUSH LOGS")

    # Check that queries were excecuted only on one replica.
    expected = f"DELETE FROM {db_name}.lwu_table WHERE x = 2\nUPDATE {db_name}.lwu_table SET y = \\'updated\\' WHERE x = 1\n"

    assert (
        node1.query(
            f"""
        SELECT query FROM system.query_log
        WHERE type = 'QueryFinish' AND query_kind IN ('Update', 'Delete') AND has(databases, '{db_name}')
        ORDER BY event_time_microseconds
    """
        )
        == expected
    )

    assert (
        node2.query(
            f"""
        SELECT query FROM system.query_log
        WHERE type = 'QueryFinish' AND query_kind IN ('Update', 'Delete') AND has(databases, '{db_name}')
        ORDER BY event_time_microseconds
    """
        )
        == ""
    )
