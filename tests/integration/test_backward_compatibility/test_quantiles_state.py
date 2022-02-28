import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, name="quantiles_state")
nodes = [
    cluster.add_instance("node1", image="yandex/clickhouse-server", tag="21.4",
                          with_zookeeper=False, stay_alive=True, with_installed_binary=True),
    cluster.add_instance("node2", with_zookeeper=False),
]


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def query_all_nodes(query : str):
    print(">>>> " , query[:100])
    for n in nodes:
        print("~~~~ " , n, n.name, query[:100])
        n.query(query)
    print("<<<< " , query[:100])


def test_quantiles_state(start_cluster):
    query_all_nodes("DROP TABLE IF EXISTS ufeszoe_default")
    query_all_nodes("DROP TABLE IF EXISTS ufeszoe_1min")

    query_all_nodes("""
        CREATE TABLE ufeszoe_default (
            `response` Int64, `duration` Nullable(Int64)
        ) ENGINE = MergeTree PARTITION BY (response % 10) ORDER BY (response)
        SETTINGS index_granularity = 8192
    """)

    query_all_nodes("""
        CREATE MATERIALIZED VIEW ufeszoe_1min (
            `response` Int64,
            `duration_quantiles` AggregateFunction(quantiles(0.5, 0.75, 0.9, 0.95, 0.99), Nullable(Int64))
        ) ENGINE = MergeTree PARTITION BY (response % 10) ORDER BY (response)
        AS SELECT response, quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)(duration) AS duration_quantiles
        FROM ufeszoe_default GROUP BY response;
    """)

    query_all_nodes("INSERT INTO ufeszoe_default SELECT number AS `response`, number AS `duration` FROM numbers(10)")

    query_all_nodes("SELECT * FROM remote('node{1..2}', default, ufeszoe_1min)")

    nodes[0].restart_with_latest_version()

    query_all_nodes("SELECT * FROM ufeszoe_1min")

    query_all_nodes("SELECT * FROM remote('node{1..2}', default, ufeszoe_1min)")

    query_all_nodes("DROP TABLE IF EXISTS ufeszoe_default")
    query_all_nodes("DROP TABLE IF EXISTS ufeszoe_1min")
