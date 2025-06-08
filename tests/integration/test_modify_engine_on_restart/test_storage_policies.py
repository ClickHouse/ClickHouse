import pytest

from helpers.cluster import ClickHouseCluster
from test_modify_engine_on_restart.common import check_flags_deleted, set_convert_flags

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
        "configs/config.d/storage_policies.xml",
    ],
    with_zookeeper=True,
    with_minio=True,
    macros={"replica": "node1"},
    stay_alive=True,
)

database_name = "modify_engine_storage_policies"


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
    # Implicit jbod (set default in config)
    q(
        ch1,
        "CREATE TABLE jbod_imp ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;",
    )

    # Explicit jbod
    q(
        ch1,
        """
        CREATE TABLE jbod_exp ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A
        SETTINGS storage_policy='jbod';
        """,
    )

    # s3
    q(
        ch1,
        """
        CREATE TABLE s3 ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A
        SETTINGS storage_policy='s3';
        """,
    )

    # Default
    q(
        ch1,
        """
        CREATE TABLE default ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A
        SETTINGS storage_policy='default';
        """,
    )


def check_tables(converted):
    engine_prefix = ""
    if converted:
        engine_prefix = "Replicated"

    assert (
        q(
            ch1,
            f"SELECT name, engine FROM system.tables WHERE database = '{database_name}'",
        ).strip()
        == f"default\t{engine_prefix}MergeTree\njbod_exp\t{engine_prefix}MergeTree\njbod_imp\t{engine_prefix}MergeTree\ns3\t{engine_prefix}MergeTree"
    )


def test_modify_engine_on_restart(started_cluster):
    ch1.query("CREATE DATABASE " + database_name)

    create_tables()

    check_tables(False)

    ch1.restart_clickhouse()

    check_tables(False)

    set_convert_flags(ch1, database_name, ["default", "jbod_exp", "jbod_imp", "s3"])

    ch1.restart_clickhouse()

    check_flags_deleted(ch1, database_name, ["default", "jbod_exp", "jbod_imp", "s3"])

    check_tables(True)
