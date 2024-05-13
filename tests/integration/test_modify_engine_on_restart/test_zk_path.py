import pytest
from test_modify_engine_on_restart.common import (
    check_flags_deleted,
    get_table_path,
    set_convert_flags,
)
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters_zk_path.xml",
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


def q(node, query, database=database_name):
    return node.query(database=database, sql=query)


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
            "ReplicatedReplacingMergeTree(\\'/clickhouse/\\\\\\'/{database}/{table}/{uuid}\\', \\'{replica}\\', D)"
        )
    )
    assert (
        q(
            ch1,
            f"SELECT engine_full FROM system.tables WHERE database = '{database_name}' and name = 'collapsing_ver'",
        )
        .strip()
        .startswith(
            "ReplicatedVersionedCollapsingMergeTree(\\'/clickhouse/\\\\\\'/{database}/{table}/{uuid}\\', \\'{replica}\\', Sign, Version)"
        )
    )


def test_modify_engine_on_restart_with_unusual_path(started_cluster):
    ch1.query("CREATE DATABASE " + database_name)

    create_tables()

    set_convert_flags(ch1, database_name, ["replacing_ver", "collapsing_ver"])

    ch1.restart_clickhouse()

    check_flags_deleted(ch1, database_name, ["replacing_ver", "collapsing_ver"])

    check_tables()


def test_modify_engine_fails_if_zk_path_exists(started_cluster):
    database_name = "zk_path"
    ch1.query("CREATE DATABASE " + database_name + " ON CLUSTER cluster")

    q(
        ch1,
        "CREATE TABLE already_exists_1 ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A;",
        database_name,
    )
    uuid = q(
        ch1,
        f"SELECT uuid FROM system.tables WHERE table = 'already_exists_1' and database = '{database_name}'",
        database_name,
    ).strip("'[]\n")

    q(
        ch1,
        f"CREATE TABLE already_exists_2 ( A Int64, D Date, S String ) ENGINE ReplicatedMergeTree('/clickhouse/\\'/{database_name}/already_exists_1/{uuid}', 'r2') PARTITION BY toYYYYMM(D) ORDER BY A;",
        database_name,
    )

    set_convert_flags(ch1, database_name, ["already_exists_1"])

    table_data_path = get_table_path(ch1, "already_exists_1", database_name)

    ch1.stop_clickhouse()
    ch1.start_clickhouse(retry_start=False, expected_to_fail=True)

    # Check if we can cancel convertation
    ch1.exec_in_container(
        [
            "bash",
            "-c",
            f"rm {table_data_path}convert_to_replicated",
        ]
    )
    ch1.start_clickhouse()
