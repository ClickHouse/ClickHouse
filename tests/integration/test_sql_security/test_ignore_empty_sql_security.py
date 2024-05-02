#!/usr/bin/env python3
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node1",
    main_configs=["configs/attach_materialized_views_with_sql_security_none.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_load_mv_with_security_none(started_cluster: ClickHouseCluster):
    node.query("CREATE TABLE test_table (s String) ENGINE = MergeTree ORDER BY s")
    node.query(
        "CREATE MATERIALIZED VIEW test_mv_1 (s String) ENGINE = MergeTree ORDER BY s AS SELECT * FROM test_table"
    )
    node.query("INSERT INTO test_table VALUES ('foo'), ('bar')")

    node.query("CREATE USER test_user")
    node.query("GRANT SELECT ON test_mv_1 TO test_user")

    with pytest.raises(Exception, match="Not enough privileges"):
        node.query("SELECT count() FROM test_mv_1", user="test_user")

    node.replace_in_config(
        "/etc/clickhouse-server/config.d/ignore_empty_sql_security_in_create_view_query.xml",
        "0",
        "1",
    )

    node.restart_clickhouse()

    assert node.query("SELECT count() FROM test_mv_1", user="test_user") == "2\n"
