import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/max_num_limit.xml"],
    stay_alive=True,
)

config = """<clickhouse>
    <max_materialized_views_count_for_table>2</max_materialized_views_count_for_table>
</clickhouse>
"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for _, node in cluster.instances.items():
            node.query(
                f"""
                CREATE TABLE test_tb (a String) ENGINE = MergeTree ORDER BY a;
                """
            )
        yield cluster
    finally:
        cluster.shutdown()


def test_limit_materialized_view_count(started_cluster):
    node.query(
        "CREATE MATERIALIZED VIEW test_view1 ENGINE = MergeTree ORDER BY a AS SELECT * FROM test_tb;"
    )
    assert "Too many materialized views" in node.query_and_get_error(
        "CREATE MATERIALIZED VIEW test_view2 ENGINE = MergeTree ORDER BY a AS SELECT * FROM test_tb;"
    )

    node.replace_config("/etc/clickhouse-server/config.d/max_num_limit.xml", config)
    node.restart_clickhouse()

    node.query(
        "CREATE MATERIALIZED VIEW test_view2 ENGINE = MergeTree ORDER BY a AS SELECT * FROM test_tb;"
    )
    assert "Too many materialized views" in node.query_and_get_error(
        "CREATE MATERIALIZED VIEW test_view3 ENGINE = MergeTree ORDER BY a AS SELECT * FROM test_tb;"
    )
