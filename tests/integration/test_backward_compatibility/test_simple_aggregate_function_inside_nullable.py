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
# Stays on an older version, one that accepts `NULL` after the type in `ALTER`.
old = cluster.add_instance(
    "old",
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag="26.8",
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
    # The type is also kept in a typed path of `JSON` and in an element of an unflattened `Nested`.
    node.query(
        "CREATE TABLE j (j JSON(x Nullable(SimpleAggregateFunction(sum, UInt64)))) ENGINE = MergeTree ORDER BY tuple()",
        settings={"enable_json_type": 1},
    )
    node.query("""INSERT INTO j VALUES ('{"x" : 42}')""")
    node.query(
        "CREATE TABLE nested (n Nested(x Nullable(SimpleAggregateFunction(sum, UInt64)))) ENGINE = MergeTree ORDER BY tuple()",
        settings={"flatten_nested": 0},
    )
    node.query("INSERT INTO nested SELECT [tuple(42)]")
    variant = {"allow_experimental_variant_type": 1, "allow_experimental_dynamic_type": 1}
    node.query(
        "CREATE TABLE v (k UInt32, v Variant(SimpleAggregateFunction(any, UInt64), String)) ENGINE = MergeTree ORDER BY k",
        settings=variant,
    )
    node.query(
        "INSERT INTO v SELECT 1, CAST(5, 'SimpleAggregateFunction(any, UInt64)')",
        settings=variant,
    )
    node.query("INSERT INTO v VALUES (2, 'x')", settings=variant)
    # A `Variant` alternative or a `Dynamic` value keeps its stored name, which locates its data.
    node.query("CREATE DATABASE bad")
    node.query(
        "CREATE TABLE bad.v (v Variant(LowCardinality(SimpleAggregateFunction(any, String)), UInt64)) "
        "ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0",
        settings=variant,
    )
    node.query(
        "INSERT INTO bad.v SELECT CAST('a', 'LowCardinality(SimpleAggregateFunction(any, String))')",
        settings=variant,
    )
    node.query(
        "CREATE TABLE bad.d (d Dynamic) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0",
        settings=variant,
    )
    node.query(
        "INSERT INTO bad.d SELECT CAST([CAST(1, 'Nullable(SimpleAggregateFunction(sum, UInt64))')], 'Dynamic')",
        settings=variant,
    )

    for replica in [old, node]:
        replica.query(f"CREATE DATABASE rdb ENGINE = Replicated('/clickhouse/databases/rdb', 's', '{replica.name}')")
    node.query("CREATE TABLE rdb.t (k UInt32, m SimpleAggregateFunction(sum, UInt64)) ENGINE = MergeTree ORDER BY k")
    node.query("INSERT INTO rdb.t VALUES (1, 10)")

    old.query(
        "CREATE TABLE f1 (k UInt32, m Nullable(SimpleAggregateFunction(sum, UInt64))) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/f', '1') ORDER BY k"
    )
    old.query("INSERT INTO f1 VALUES (1, 42)")

    # Queued by the older version while `node` is down, replayed by `node` after the upgrade.
    def alter_while_stopped(_):
        no_wait = {"distributed_ddl_task_timeout": 0}
        old.query("ALTER TABLE rdb.t MODIFY COLUMN m Nullable(SimpleAggregateFunction(sum, UInt64))", settings=no_wait)
        old.query("ALTER TABLE rdb.t ADD COLUMN x SimpleAggregateFunction(sum, UInt64) NULL", settings=no_wait)
        old.query("ALTER TABLE rdb.t MODIFY COLUMN x SimpleAggregateFunction(sum, UInt64) NULL COMMENT 'x'", settings=no_wait)
        old.query("ALTER TABLE rdb.t MODIFY COMMENT 'later'", settings=no_wait)

    node.restart_with_latest_version(callback_onstop=alter_while_stopped)

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
    # The fetched parts keep their `columns.txt`, so the new replica publishes the same part headers as the old one:
    # a replica of the older version, which reads that file as written, compares it with the header of its source.
    assert (
        node.query(
            "SELECT count(), uniqExact(name), uniqExact((name, value)) FROM system.zookeeper "
            "WHERE path IN ('/clickhouse/tables/r/replicas/1/parts', '/clickhouse/tables/r/replicas/2/parts')"
        )
        == "2\t1\t1\n"
    )
    assert node.query("SELECT j.x, toTypeName(j.x) FROM j") == "42\tSimpleAggregateFunction(sum, Nullable(UInt64))\n"
    assert (
        node.query("SELECT n.x, toTypeName(n.x) FROM nested")
        == "[42]\tArray(SimpleAggregateFunction(sum, Nullable(UInt64)))\n"
    )
    node.query("OPTIMIZE TABLE t FINAL")
    assert node.query("SELECT k, m, s, n FROM t") == "1\t30\ta\t20\n"
    assert node.query("SELECT k, m FROM mut") == "1\t10\n"
    assert (
        node.query("SELECT k, v, v.`SimpleAggregateFunction(any, UInt64)` FROM v ORDER BY k")
        == "1\t5\t5\n2\tx\t\\N\n"
    )
    assert "DataTypeLowCardinality is supported only" in node.query_and_get_error("SELECT * FROM bad.v")
    assert "cannot be inside Nullable type" in node.query_and_get_error("SELECT * FROM bad.d")
    assert (
        node.query(
            "SELECT count() FROM system.detached_parts WHERE database = 'default'"
        )
        == "0\n"
    )

    assert_eq_with_retry(node, "SELECT comment FROM system.tables WHERE database = 'rdb' AND name = 't'", "later")
    assert (
        node.query("SELECT m, toTypeName(m), x, toTypeName(x) FROM rdb.t")
        == "10\tSimpleAggregateFunction(sum, Nullable(UInt64))\t\\N\tSimpleAggregateFunction(sum, Nullable(UInt64))\n"
    )
    assert node.query("SELECT comment FROM system.columns WHERE database = 'rdb' AND table = 't' AND name = 'x'") == "x\n"

    # An unchanged part fetched from the older version is fetched back by a replica of the older version.
    node.query(
        "CREATE TABLE f2 (k UInt32, m SimpleAggregateFunction(sum, Nullable(UInt64))) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/f', '2') ORDER BY k"
    )
    assert_eq_with_retry(node, "SELECT k, m FROM f2", "1\t42")
    old.query("DROP TABLE f1 SYNC")
    old.query(
        "CREATE TABLE f3 (k UInt32, m Nullable(SimpleAggregateFunction(sum, UInt64))) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/f', '3') ORDER BY k"
    )
    assert_eq_with_retry(old, "SELECT k, m FROM f3", "1\t42")
