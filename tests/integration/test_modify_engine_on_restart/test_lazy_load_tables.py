import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import check_flags_deleted, set_convert_flags

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
        "configs/config.d/storage_policies_jbod.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node1"},
    stay_alive=True,
)

database_name = "modify_engine_lazy_load"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(node, query):
    return node.query(database=database_name, sql=query)


def test_modify_engine_on_restart_with_lazy_load_tables(started_cluster):
    # A table of a database with `lazy_load_tables` comes up as a stand-in that materializes the real
    # storage only on first access, so the second phase of the conversion used to skip it: the table
    # stayed replicated with no metadata in ZooKeeper - read-only - and the flag was never removed, so
    # no further restart could finish the conversion either.
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(
        f"CREATE DATABASE {database_name} ENGINE = Atomic SETTINGS lazy_load_tables = 1"
    )

    q(ch1, "CREATE TABLE mt ( A Int64, D Date ) ENGINE = MergeTree() ORDER BY A;")
    q(ch1, "INSERT INTO mt SELECT number, today() FROM numbers(100);")

    set_convert_flags(ch1, database_name, ["mt"])

    ch1.restart_clickhouse()

    check_flags_deleted(ch1, database_name, ["mt"])

    assert (
        q(
            ch1,
            f"SELECT engine FROM system.tables WHERE database = '{database_name}' AND name = 'mt'",
        ).strip()
        == "ReplicatedMergeTree"
    )

    # The conversion finished, so the table is writable and its data is intact.
    q(ch1, "INSERT INTO mt SELECT number, today() FROM numbers(100, 100);")
    assert q(ch1, "SELECT count() FROM mt").strip() == "200"

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")


def test_modify_engine_on_restart_with_lazy_load_tables_and_storage_policy(
    started_cluster,
):
    # The flag of a table with a non-default `storage_policy` lives on the first disk of that policy, not
    # on the database disk. A stand-in cannot be asked for its policy, so the second phase has to resolve
    # it from the `CREATE TABLE` query the same way the first phase did - otherwise the flag is missed,
    # the table stays read-only and the flag lingers forever.
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(
        f"CREATE DATABASE {database_name} ENGINE = Atomic SETTINGS lazy_load_tables = 1"
    )

    q(
        ch1,
        """
        CREATE TABLE mt_jbod ( A Int64, D Date ) ENGINE = MergeTree() ORDER BY A
        SETTINGS storage_policy = 'jbod';
        """,
    )
    q(ch1, "INSERT INTO mt_jbod SELECT number, today() FROM numbers(100);")

    set_convert_flags(ch1, database_name, ["mt_jbod"])

    ch1.restart_clickhouse()

    check_flags_deleted(ch1, database_name, ["mt_jbod"])

    assert (
        q(
            ch1,
            f"SELECT engine FROM system.tables WHERE database = '{database_name}' AND name = 'mt_jbod'",
        ).strip()
        == "ReplicatedMergeTree"
    )

    q(ch1, "INSERT INTO mt_jbod SELECT number, today() FROM numbers(100, 100);")
    assert q(ch1, "SELECT count() FROM mt_jbod").strip() == "200"

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
