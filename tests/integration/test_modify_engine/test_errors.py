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

database_name = "modify_engine_errors"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(query):
    return ch1.query(
        database=database_name, sql=query, settings={"allow_modify_engine_query": 1}
    )


def q_error(query):
    return ch1.query_and_get_error(
        database=database_name, sql=query, settings={"allow_modify_engine_query": 1}
    )


def create_tables():
    zk_path = "/clickhouse/tables/{table}/{shard}"
    replica = "{replica}"

    q(
        "CREATE TABLE mt ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A",
    )

    q(
        f"CREATE TABLE rmt ( A Int64, D Date, S String ) ENGINE ReplicatedMergeTree('{zk_path}', '{replica}') PARTITION BY toYYYYMM(D) ORDER BY A",
    )

    q("CREATE TABLE log ( A Int64, D Date, S String ) ENGINE Log")

    ch1.query(
        database="ord",
        sql="CREATE TABLE mt ( A Int64, D Date, S String ) ENGINE MergeTree() PARTITION BY toYYYYMM(D) ORDER BY A",
    )


def check_tables():
    # Already converted
    assert "Table is already not replicated" in q_error(
        "ALTER TABLE mt MODIFY ENGINE TO NOT REPLICATED"
    )
    assert "Table is already replicated" in q_error(
        "ALTER TABLE rmt MODIFY ENGINE TO REPLICATED"
    )

    # Wrong engine
    assert "Only MergeTree and ReplicatedMergeTree" in q_error(
        "ALTER TABLE log MODIFY ENGINE TO NOT REPLICATED"
    )
    assert "Only MergeTree and ReplicatedMergeTree" in q_error(
        "ALTER TABLE log MODIFY ENGINE TO REPLICATED"
    )

    # ON CLUSTER
    assert "Modify engine on cluster is not implemented" in q_error(
        "ALTER TABLE log ON CLUSTER cluster MODIFY ENGINE TO REPLICATED"
    )

    # Not atomic database
    assert (
        "Table engine conversion is supported only for Atomic databases"
        in ch1.query_and_get_error(
            database="ord",
            sql="ALTER TABLE mt MODIFY ENGINE TO REPLICATED",
            settings={"allow_modify_engine_query": 1},
        )
    )

    # Setting not set
    assert "MODIFY ENGINE query is not allowed" in ch1.query_and_get_error(
        database=database_name, sql="ALTER TABLE mt MODIFY ENGINE TO REPLICATED"
    )


def test_modify_engine(started_cluster):
    ch1.query("CREATE DATABASE " + database_name)
    ch1.query(
        "CREATE DATABASE ord ENGINE = Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )

    create_tables()

    check_tables()
