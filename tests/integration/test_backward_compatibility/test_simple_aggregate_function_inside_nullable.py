import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_stored_nullable_simple_aggregate_function(start_cluster):
    # Older versions allowed `SimpleAggregateFunction` inside `Nullable` and `LowCardinality`.
    node.query(
        "CREATE TABLE t (k UInt32, m Nullable(SimpleAggregateFunction(sum, UInt64)), s LowCardinality(SimpleAggregateFunction(any, String)), "
        "n Nullable(SimpleAggregateFunction(max, SimpleAggregateFunction(sum, UInt64)))) "
        "ENGINE = AggregatingMergeTree ORDER BY k"
    )
    node.query(
        "CREATE TABLE r1 (k UInt32, m Nullable(SimpleAggregateFunction(sum, UInt64))) "
        "ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/r', '1') ORDER BY k"
    )
    node.query(
        "CREATE TABLE mut (k UInt32, m SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k"
    )
    node.query("SYSTEM STOP MERGES t")
    node.query("INSERT INTO t VALUES (1, 10, 'a', 10)")
    node.query("INSERT INTO t VALUES (1, 20, 'a', 20)")
    node.query("INSERT INTO r1 VALUES (1, 10), (2, 20)")
    node.query("INSERT INTO mut VALUES (1, 10)")
    node.query(
        "ALTER TABLE mut MODIFY COLUMN m Nullable(SimpleAggregateFunction(sum, UInt64)) SETTINGS mutations_sync = 2"
    )

    node.restart_with_latest_version()

    assert (
        node.query(
            "SELECT DISTINCT type FROM system.columns WHERE database = 'default' AND name = 'm'"
        )
        == "SimpleAggregateFunction(sum, Nullable(UInt64))\n"
    )
    assert (
        node.query("SELECT toTypeName(s), toTypeName(n) FROM t LIMIT 1")
        == "SimpleAggregateFunction(any, LowCardinality(String))\tSimpleAggregateFunction(max, SimpleAggregateFunction(sum, Nullable(UInt64)))\n"
    )

    # The parts written by the older version are fetched by a new replica, and their merges apply the function.
    node.query(
        "CREATE TABLE r2 (k UInt32, m SimpleAggregateFunction(sum, Nullable(UInt64))) "
        "ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/r', '2') ORDER BY k"
    )
    assert_eq_with_retry(node, "SELECT sum(m) FROM r2", "30")
    node.query("OPTIMIZE TABLE t FINAL")
    assert node.query("SELECT k, m, s, n FROM t") == "1\t30\ta\t20\n"
    assert node.query("SELECT k, m FROM mut") == "1\t10\n"
    assert (
        node.query(
            "SELECT count() FROM system.detached_parts WHERE database = 'default'"
        )
        == "0\n"
    )
