import pytest

from helpers.cluster import ClickHouseCluster
from .common import run_test

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    macros={"replica": 1, "shard": 1},
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    macros={"replica": 2, "shard": 1},
)
nodes = [node1, node2]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree"])
def test_validate_only_initial_alter_query_replicated_database(started_cluster, engine):
    run_test(node1, node2, "Replicated('/clickhouse/databases/{database_name}', '{{shard}}', '{{replica}}')", engine)


def test_projection_column_list_replay_without_initiator_settings(started_cluster):
    database_name = "test_projection_column_list_replay"
    for node in nodes:
        node.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")

    node1.query(
        f"CREATE DATABASE {database_name} "
        f"ENGINE = Replicated('/clickhouse/databases/{database_name}', 's1', 'r1')"
    )
    node2.query(
        f"CREATE DATABASE {database_name} "
        f"ENGINE = Replicated('/clickhouse/databases/{database_name}', 's1', 'r2')"
    )

    assert node2.query(
        "SELECT value FROM system.settings "
        "WHERE name = 'allow_projection_column_list_in_replicated_metadata'"
    ) == "0\n"

    # DDL log format 1 does not carry the initiator's session settings. The secondary must
    # replay both statements even though its own compatibility setting remains disabled.
    ddl_settings = {
        "allow_projection_column_list_in_replicated_metadata": 1,
        "distributed_ddl_entry_format_version": 1,
    }
    node1.query(
        f"CREATE TABLE {database_name}.t "
        "(x UInt64, PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x)) "
        "ENGINE = MergeTree ORDER BY x",
        settings=ddl_settings,
    )
    node1.query(
        f"ALTER TABLE {database_name}.t "
        "ADD PROJECTION q (x CODEC(LZ4)) AS (SELECT x ORDER BY x)",
        settings=ddl_settings,
    )

    node2.query(f"SYSTEM SYNC DATABASE REPLICA {database_name}", timeout=30)

    for node in nodes:
        assert node.query(
            "SELECT count() FROM system.projections "
            f"WHERE database = '{database_name}' AND table = 't'"
        ) == "2\n"
