import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import set_convert_flags

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


def remove_convert_flag(table):
    from test_modify_engine_on_restart.common import get_table_path

    ch1.exec_in_container(
        [
            "bash",
            "-c",
            f"rm {get_table_path(ch1, table, database_name)}convert_to_replicated",
        ]
    )


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

    # The illegal conversion is rejected during startup, so the server refuses to start.
    cannot_start = False
    try:
        ch1.restart_clickhouse()
    except Exception:
        cannot_start = True
    assert cannot_start

    # Crucially, the metadata was NOT rewritten: after removing the flag the table loads with its
    # original MergeTree engine and its data intact (this is the load path that was broken before
    # the fix, where the metadata had been corrupted to ReplicatedMergeTree + UNIQUE KEY).
    remove_convert_flag("uk")
    ch1.restart_clickhouse()

    assert (
        q(ch1, "SELECT engine FROM system.tables WHERE name = 'uk'").strip()
        == "MergeTree"
    )
    assert q(ch1, "SELECT count() FROM uk").strip() == "2"

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
