import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import check_flags_deleted, set_convert_flags

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters_unusual.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
    macros={"replica": "node1"},
    stay_alive=True,
)

database_name = "modify_engine_unusual_path"


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
    # Check one argument
    q(
        ch1,
        "CREATE TABLE replacing_ver ( A Int64, D Date, S String ) ENGINE = ReplacingMergeTree(D) PARTITION BY toYYYYMM(D) ORDER BY A",
    )

    # Check more than one argument
    q(
        ch1,
        "CREATE TABLE collapsing_ver ( ID UInt64, Sign Int8, Version UInt8 ) ENGINE = VersionedCollapsingMergeTree(Sign, Version) ORDER BY ID",
    )


def check_tables():
    # Check tables exists
    assert (
        q(
            ch1,
            "SHOW TABLES",
        ).strip()
        == "collapsing_ver\nreplacing_ver"
    )

    # Check engines
    assert (
        q(
            ch1,
            f"SELECT engine_full FROM system.tables WHERE database = '{database_name}' and name = 'replacing_ver'",
        )
        .strip()
        .startswith(
            "ReplicatedReplacingMergeTree(\\'/lol/kek/\\\\\\'/{uuid}\\', \\'{replica}\\', D)"
        )
    )
    assert (
        q(
            ch1,
            f"SELECT engine_full FROM system.tables WHERE database = '{database_name}' and name = 'collapsing_ver'",
        )
        .strip()
        .startswith(
            "ReplicatedVersionedCollapsingMergeTree(\\'/lol/kek/\\\\\\'/{uuid}\\', \\'{replica}\\', Sign, Version)"
        )
    )


def test_modify_engine_on_restart_with_unusual_path(started_cluster):
    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
    ch1.query(f"CREATE DATABASE {database_name}")

    create_tables()

    set_convert_flags(ch1, database_name, ["replacing_ver", "collapsing_ver"])

    ch1.restart_clickhouse()

    check_flags_deleted(ch1, database_name, ["replacing_ver", "collapsing_ver"])

    check_tables()

    ch1.query(f"DROP DATABASE IF EXISTS {database_name} SYNC")
