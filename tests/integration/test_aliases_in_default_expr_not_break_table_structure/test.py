import pytest
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "config/enable_keeper.xml",
        "config/users.xml",
    ],
    stay_alive=True,
    with_minio=True,
    macros={"shard": 1, "replica": 1},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("engine", ["ReplicatedMergeTree"])
def test_aliases_in_default_expr_not_break_table_structure(start_cluster, engine):
    """
    Making sure that using aliases in columns' default expressions does not lead to having different columns metadata in ZooKeeper and on disk.
    Issue: https://github.com/ClickHouse/clickhouse-private/issues/5150
    """

    data = '{"event": {"col1-key": "col1-val", "col2-key": "col2-val"}}'

    node.query(
        """
        DROP DATABASE IF EXISTS rdb;
        CREATE DATABASE rdb
        ENGINE = Replicated('/test/test_recovery_time_metric_db', '{shard}', '{replica}')
        """
    )

    node.query(
        f"""
        DROP TABLE IF EXISTS rdb.t;
        CREATE TABLE rdb.t
        (
            `data` String,
            `col1` String DEFAULT JSONExtractString(JSONExtractString(data, 'event') AS event, 'col1-key'),
            `col2` String MATERIALIZED JSONExtractString(JSONExtractString(data, 'event') AS event, 'col2-key')
        )
        ENGINE = {engine}
        ORDER BY col1
        """
    )

    node.restart_clickhouse()

    node.query(
        f"""
        INSERT INTO rdb.t (data) VALUES ('{data}');
        """
    )
    assert node.query("SELECT data FROM rdb.t").strip() == data
    assert node.query("SELECT col1 FROM rdb.t").strip() == "col1-val"
    assert node.query("SELECT col2 FROM rdb.t").strip() == "col2-val"
