import logging
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from helpers.database_disk import move_file

test_recover_staled_replica_run = 1

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1}
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

def test_warn_on_database_ending_broken_replicated_tables(started_cluster):
    database_name = "test_warn_on_database_name"
    ordinary_database_for_broken_tables = f"{database_name}_broken_tables"
    atomic_database_for_broken_tables = f"{database_name}_broken_replicated_tables"

    for node in [node1, node2]:
        node.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
        node.query(f"DROP DATABASE IF EXISTS {ordinary_database_for_broken_tables} SYNC")
        node.query(f"DROP DATABASE IF EXISTS {atomic_database_for_broken_tables} SYNC")
    node1.query(
        f"CREATE DATABASE {database_name} ENGINE = Replicated('/clickhouse/databases/{database_name}', 'shard1', 'replica1');"
    )
    started_cluster.get_kazoo_client("zoo1").set(
        f"/clickhouse/databases/{database_name}/logs_to_keep", b"4"
    )
    node2.query(
        f"CREATE DATABASE {database_name} ENGINE = Replicated('/clickhouse/databases/{database_name}', 'shard1', 'replica2');"
    )

    settings = {"distributed_ddl_task_timeout": 0}
    node1.query(
        f"CREATE TABLE {database_name}.mt_0 (n Int64) ENGINE = MergeTree ORDER BY n"
    )
    node1.query(
        f"CREATE TABLE {database_name}.rmt_0 (n Int64) ENGINE = ReplicatedMergeTree ORDER BY n"
    )

    for table in ["mt_0", "rmt_0"]:
        node1.query(f"INSERT INTO {database_name}.{table} VALUES (42)")
        node2.query(f"INSERT INTO {database_name}.{table} VALUES (42)")

    node1.query(f"SYSTEM SYNC REPLICA {database_name}.rmt_0")

    metadata_path = node2.query(
        f"SELECT metadata_path FROM system.tables WHERE database='{database_name}' AND name='mt_0'"
    ).strip()

    node2.stop_clickhouse()

    # This will result in a broken table.
    logging.debug(f"Moving metadata {metadata_path}")
    move_file(node2, metadata_path, metadata_path.replace("mt_0", "mt_x"))

    for i in range(0, 8):
        node1.query(
            f"RENAME TABLE {database_name}.mt_{i} TO {database_name}.mt_{i+1}",
            settings=settings,
        )

    node2.start_clickhouse()
    query = (
        f"SELECT name, uuid, create_table_query FROM system.tables WHERE database='{database_name}'"
        "ORDER BY name SETTINGS show_table_uuid_in_table_create_query_if_not_nil=1"
    )
    expected = node1.query(query)
    assert_eq_with_retry(node2, query, expected)

    logging.debug("Result: %s", node2.query("SHOW DATABASES"))

    warning_message_format = "The database {} is probably created during recovering a lost replica. If it has no tables, it can be deleted. If it has tables, it worth to check why they were considered broken."

    # Currently the Atomic database is created as second, so that will be shown in `system.warnings`
    warning_count = node2.query(
        f"SELECT count() FROM system.warnings WHERE message = '{warning_message_format.format(atomic_database_for_broken_tables)}'"
    )
    assert warning_count == "1\n"

    # Assert that no warning about ordinary engine
    warning_about_ordinary_database = node2.query(
        "SELECT message FROM system.warnings WHERE message ILIKE '%ordinary%'"
    )
    assert warning_about_ordinary_database == ""

    # Let's drop the database that showed up in system.warnings
    node2.query(f"DROP DATABASE {atomic_database_for_broken_tables} SYNC")
    node2.restart_clickhouse()

    warning_count = node2.query(
        f"SELECT count() FROM system.warnings WHERE message = '{warning_message_format.format(ordinary_database_for_broken_tables)}'"
    )
    assert warning_count == "1\n"

    node2.query(f"DROP DATABASE {ordinary_database_for_broken_tables} SYNC")
    node2.restart_clickhouse()

    remaining_warnings = node2.query(
        f"SELECT message FROM system.warnings WHERE message_format_string = '{warning_message_format}'"
    )
    assert remaining_warnings == ""

    node1.query(f"DROP DATABASE {database_name} SYNC")
    node2.query(f"DROP DATABASE {database_name} SYNC")
