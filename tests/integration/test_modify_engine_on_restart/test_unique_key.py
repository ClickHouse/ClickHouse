import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import get_table_path, set_convert_flags

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node1"},
    stay_alive=True,
)
# Second instance with `table_readonly = 1` in the global `merge_tree` config (not the table's
# own SETTINGS). ReplicatedMergeTree rejects that setting in its constructor, so the restart-time
# converter must reject the conversion here too. This exercises the RESOLVED-settings path: the
# value comes from server config, which an AST-only check would miss.
ch2 = cluster.add_instance(
    "ch2",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
        "configs/config.d/table_readonly.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node2"},
    stay_alive=True,
)

database_name = "modify_engine_unique_key"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(node, query, **kwargs):
    return node.query(database=database_name, sql=query, **kwargs)


def test_convert_to_replicated_rejected_for_unique_key(started_cluster):
    # ReplicatedMergeTree does not support UNIQUE KEY. The restart-time converter must reject the
    # conversion BEFORE rewriting the on-disk metadata; otherwise the table is persisted as an
    # unloadable ReplicatedMergeTree + UNIQUE KEY definition and can never be loaded again. See
    # issue #110854.
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(f"CREATE DATABASE {database_name}")

    q(
        ch1,
        "CREATE TABLE uk ( A Int64 ) ENGINE = MergeTree() ORDER BY tuple() UNIQUE KEY (A)",
        settings={"allow_experimental_unique_key": 1},
    )
    q(ch1, "INSERT INTO uk VALUES (1), (2)")

    set_convert_flags(ch1, database_name, ["uk"])

    # Capture the data path while the server is still up: the illegal conversion makes the server
    # refuse to start below, so system.tables is unreachable afterwards.
    table_data_path = get_table_path(ch1, "uk", database_name)

    # The illegal conversion is rejected during startup, so the server refuses to start.
    ch1.stop_clickhouse()
    ch1.start_clickhouse(start_wait_sec=120, expected_to_fail=True)

    # Crucially, the metadata was NOT rewritten: after removing the flag the table loads with its
    # original MergeTree engine and its data intact (this is the load path that was broken before
    # the fix, where the metadata had been corrupted to ReplicatedMergeTree + UNIQUE KEY).
    ch1.exec_in_container(["rm", f"{table_data_path}convert_to_replicated"])
    ch1.start_clickhouse()

    assert (
        q(ch1, "SELECT engine FROM system.tables WHERE name = 'uk'").strip()
        == "MergeTree"
    )
    assert q(ch1, "SELECT count() FROM uk").strip() == "2"

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")


def test_convert_to_replicated_rejected_for_config_table_readonly(started_cluster):
    # ReplicatedMergeTree rejects `table_readonly = 1` in its constructor. Here the setting comes
    # from the global `merge_tree` server config, not the table's own SETTINGS, so the validation
    # must resolve the effective settings (an AST-only check would miss it). The restart-time
    # converter must reject the conversion BEFORE rewriting the on-disk metadata; otherwise the
    # table is persisted as an unloadable ReplicatedMergeTree definition. See issue #110854.
    ch2.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch2.query(f"CREATE DATABASE {database_name}")

    ch2.query(
        database=database_name,
        sql="CREATE TABLE ro ( A Int64 ) ENGINE = MergeTree() ORDER BY tuple()",
    )
    ch2.query(database=database_name, sql="INSERT INTO ro VALUES (1), (2)")

    set_convert_flags(ch2, database_name, ["ro"])

    # Capture the data path while the server is still up: the illegal conversion makes the server
    # refuse to start below, so system.tables is unreachable afterwards.
    table_data_path = get_table_path(ch2, "ro", database_name)

    # The illegal conversion is rejected during startup, so the server refuses to start.
    ch2.stop_clickhouse()
    ch2.start_clickhouse(start_wait_sec=120, expected_to_fail=True)

    # Crucially, the metadata was NOT rewritten: after removing the flag the table loads with its
    # original MergeTree engine and its data intact.
    ch2.exec_in_container(["rm", f"{table_data_path}convert_to_replicated"])
    ch2.start_clickhouse()

    assert (
        ch2.query(
            database=database_name,
            sql="SELECT engine FROM system.tables WHERE name = 'ro'",
        ).strip()
        == "MergeTree"
    )
    assert (
        ch2.query(database=database_name, sql="SELECT count() FROM ro").strip() == "2"
    )

    ch2.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
