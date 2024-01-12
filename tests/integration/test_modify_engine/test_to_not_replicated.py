import pytest
from helpers.cluster import ClickHouseCluster

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

database_name = "modify_engine"


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
    zk_path = "/clickhouse/tables/{table}/{shard}"
    replica = "{replica}"

    q(
        ch1,
        f"CREATE TABLE rmt ( A Int64, D Date, S String ) ENGINE ReplicatedMergeTree('{zk_path}', '{replica}') PARTITION BY toYYYYMM(D) ORDER BY A",
    )

    q(ch1, "INSERT INTO rmt SELECT number, today(), '' FROM numbers(1e6)")
    q(ch1, "INSERT INTO rmt SELECT number, today()-60, '' FROM numbers(1e5)")

    # ReplacingMergeTree table that will be converted to check unusual engine kinds
    q(
        ch1,
        f"CREATE TABLE replacing ( A Int64, D Date, S String ) ENGINE ReplicatedReplacingMergeTree('{zk_path}', '{replica}', D) PARTITION BY toYYYYMM(D) ORDER BY A",
    )

    q(ch1, "INSERT INTO replacing SELECT number, today(), '' FROM numbers(1e6)")
    q(ch1, "INSERT INTO replacing SELECT number, today()-60, '' FROM numbers(1e5)")

    # Check one argument
    q(
        ch1,
        f"CREATE TABLE replacing_ver ( A Int64, D Date, S String ) ENGINE = ReplicatedReplacingMergeTree('{zk_path}', '{replica}', D) PARTITION BY toYYYYMM(D) ORDER BY A",
    )

    # Check more than one argument
    q(
        ch1,
        f"CREATE TABLE collapsing_ver ( ID UInt64, Sign Int8, Version UInt8 ) ENGINE = ReplicatedVersionedCollapsingMergeTree('{zk_path}', '{replica}', Sign, Version) ORDER BY ID",
    )


def convert_tables():
    q(
        ch1,
        "ALTER TABLE rmt MODIFY ENGINE TO NOT REPLICATED"
    )
    q(
        ch1,
        "ALTER TABLE replacing MODIFY ENGINE TO NOT REPLICATED"
    )
    q(
        ch1,
        "ALTER TABLE replacing_ver MODIFY ENGINE TO NOT REPLICATED"
    )
    q(
        ch1,
        "ALTER TABLE collapsing_ver MODIFY ENGINE TO NOT REPLICATED"
    )

def check_tables():
    # Check tables exists
    assert (
        q(
            ch1,
            "SHOW TABLES",
        ).strip()
        == "collapsing_ver\nreplacing\nreplacing_ver\nrmt"
    )

    # Check engines
    assert (
        q(
            ch1,
            f"SELECT engine_full FROM system.tables WHERE database = '{database_name}' and name = 'rmt'",
        )
        .strip()
        .startswith(
            "MergeTree"
        )
    )
    assert (
        q(
            ch1,
            f"SELECT engine_full FROM system.tables WHERE database = '{database_name}' and name = 'replacing'",
        )
        .strip()
        .startswith(
            "ReplacingMergeTree"
        )
    )

    # Check engines for tables with arguments
    assert (
        q(
            ch1,
            f"SELECT engine_full FROM system.tables WHERE database = '{database_name}' and name = 'replacing_ver'",
        )
        .strip()
        .startswith(
            "ReplacingMergeTree(D)"
        )
    )
    assert (
        q(
            ch1,
            f"SELECT engine_full FROM system.tables WHERE database = '{database_name}' and name = 'collapsing_ver'",
        )
        .strip()
        .startswith(
            "VersionedCollapsingMergeTree(Sign, Version)"
        )
    )

    # Check values
    for table in ["rmt", "replacing"]:
        assert (
            q(
                ch1,
                f"SELECT count() FROM {table}",
            ).strip()
            == "1100000"
        )


def test_modify_engine(started_cluster):
    ch1.query("CREATE DATABASE " + database_name)

    create_tables()

    convert_tables()

    check_tables()
