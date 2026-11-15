import re

import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import check_flags_deleted, set_convert_flags

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
        "configs/config.d/transactions.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node1"},
    stay_alive=True,
    # Transactions refuse to start unless Keeper advertises these.
    keeper_required_feature_flags=[
        "filtered_list",
        "multi_read",
        "list_with_stat_and_data",
        "check_stat",
    ],
)

database_name = "modify_engine_on_ordinary"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(node, query):
    return node.query(database=database_name, sql=query)


def create_tables():
    q(
        ch1,
        "CREATE TABLE mt ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;",
    )


def check_tables(engine):
    # Check tables exists
    assert (
        q(
            ch1,
            "SHOW TABLES",
        ).strip()
        == "mt"
    )

    # Check engines
    assert (
        q(
            ch1,
            f"SELECT name, engine FROM system.tables WHERE database = '{database_name}'",
        ).strip()
        == f"mt\t{engine}"
    )

    if engine == "ReplicatedMergeTree":
        zookeeper_path = q(ch1, "SELECT zookeeper_path FROM system.replicas WHERE table = 'mt'").strip()
        assert re.fullmatch(
            r"/clickhouse/tables/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/01",
            zookeeper_path,
        )


def test_modify_engine_on_restart_ordinary_database(started_cluster):
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(
        sql=f"CREATE DATABASE {database_name} ENGINE = Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )

    create_tables()

    check_tables("MergeTree")

    set_convert_flags(ch1, database_name, ["mt"])

    ch1.restart_clickhouse()

    check_flags_deleted(ch1, database_name, ["mt"])
    check_tables("ReplicatedMergeTree")

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")


def test_attach_as_replicated_ordinary_database(started_cluster):
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(
        sql=f"CREATE DATABASE {database_name} ENGINE = Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )

    create_tables()
    check_tables("MergeTree")

    q(ch1, "DETACH TABLE mt")
    q(ch1, "ATTACH TABLE mt AS REPLICATED")

    check_tables("ReplicatedMergeTree")
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")


def test_modify_engine_on_restart_clears_transaction_metadata(started_cluster):
    # Parts written under a transaction carry `txn_version.txt`. A converted `ReplicatedMergeTree` that still
    # finds such a file enables transactions for the whole table, and every replicated merge, which runs
    # without a transaction, is then cancelled. The conversion on restart removes those files, the same way
    # `ATTACH TABLE ... AS REPLICATED` does. Transactions refuse to touch a table of an `Ordinary` database,
    # so the parts are written in an `Atomic` one and the table is renamed afterwards, which keeps them.
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query("DROP TABLE IF EXISTS default.mt_txn SYNC")
    ch1.query(
        sql=f"CREATE DATABASE {database_name} ENGINE = Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )

    ch1.query(
        "CREATE TABLE default.mt_txn ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A"
    )
    for i in range(2):
        ch1.query(
            sql=f"INSERT INTO default.mt_txn VALUES ({i}, '2024-01-01', 'a')",
            settings={"implicit_transaction": 1, "async_insert": 0},
        )
    ch1.query(f"RENAME TABLE default.mt_txn TO {database_name}.mt")

    # The parts keep their transaction metadata after the move.
    assert (
        q(
            ch1,
            f"SELECT count() FROM system.parts WHERE database = '{database_name}' AND table = 'mt' AND active AND creation_tid.1 != 1",
        ).strip()
        == "2"
    )

    set_convert_flags(ch1, database_name, ["mt"])
    ch1.restart_clickhouse()

    check_flags_deleted(ch1, database_name, ["mt"])
    check_tables("ReplicatedMergeTree")

    # A replicated merge of those parts goes through, instead of being cancelled for running without a
    # transaction. The wait is bounded: a table that kept its transaction metadata cancels the merge over and
    # over, so this query would otherwise only end with the client timeout.
    ch1.query(database=database_name, sql="OPTIMIZE TABLE mt FINAL", timeout=120)
    assert not ch1.contains_in_log(
        "Cancelling merge, because it was done without starting transaction"
    )
    assert (
        q(
            ch1,
            f"SELECT count() FROM system.parts WHERE database = '{database_name}' AND table = 'mt' AND active",
        ).strip()
        == "1"
    )
    assert q(ch1, "SELECT count() FROM mt").strip() == "2"

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
