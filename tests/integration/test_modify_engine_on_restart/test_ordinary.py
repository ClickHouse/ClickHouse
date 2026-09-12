import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import check_flags_deleted, set_convert_flags

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
        assert (
            q(
                ch1,
                "SELECT zookeeper_path FROM system.replicas WHERE table = 'mt'",
            ).strip()
            == f"/clickhouse/tables/{database_name}/mt"
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
