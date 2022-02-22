import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, name="quantiles_state")
node1 = cluster.add_instance("node1", image="yandex/clickhouse-server", tag="21.4",
                             with_zookeeper=False, stay_alive=True, with_installed_binary=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_quantiles_state(start_cluster):
    node1.query("DROP TABLE IF EXISTS ufeszoe_default")
    node1.query("DROP TABLE IF EXISTS ufeszoe_1min")

    node1.query("""
        CREATE TABLE ufeszoe_default (`response` Int64, `duration` Nullable(Int64))
        ENGINE = MergeTree
        PARTITION BY (response % 10)
        ORDER BY (response)
        SETTINGS index_granularity = 8192
    """)

    node1.query("""
        CREATE MATERIALIZED VIEW ufeszoe_1min (
            `response` Int64,
            `duration_quantiles` AggregateFunction(quantiles(0.5, 0.75, 0.9, 0.95, 0.99), Nullable(Int64))
        ) ENGINE = MergeTree
        PARTITION BY (response % 10)
        ORDER BY (response)
        AS SELECT response, quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)(duration) AS duration_quantiles
        FROM ufeszoe_default GROUP BY response;
    """)

    node1.query("INSERT INTO ufeszoe_default SELECT number AS `response`, number AS `duration` FROM numbers(100)")

    node1.restart_with_latest_version()

    node1.query("SELECT * FROM ufeszoe_1min")

    node1.query("DROP TABLE IF EXISTS ufeszoe_default")
    node1.query("DROP TABLE IF EXISTS ufeszoe_1min")
