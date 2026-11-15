import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    image="clickhouse/clickhouse-server",
    tag="25.8",
    with_installed_binary=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_variant_escaping_merge_tree_compatibility(started_cluster):
    def create_tables(node, other_node_name):
        node.query(
            "DROP TABLE IF EXISTS repro1 SYNC; CREATE TABLE repro1 (id UInt64, s1 String, s2 LowCardinality(String)) ENGINE = MergeTree ORDER BY id"
        )
        node.query(
            "INSERT INTO repro1 SELECT number, 'somestring', 'somestring' FROM numbers(10000)"
        )
        node.query(
            f"CREATE TABLE IF NOT EXISTS dist_repro1 (id UInt64, s1 String, s2 LowCardinality(String)) AS remote('{other_node_name}', 'default.repro1', 'default', '')"
        )
        node.query(
            "CREATE TABLE IF NOT EXISTS global_repro1 (id UInt64, s1 String, s2 LowCardinality(String)) ENGINE = Merge('default', '.*repro1')"
        )

    node.query("""
        DROP TABLE IF EXISTS test_compact;
        DROP TABLE IF EXISTS test_wide;
        CREATE TABLE test_compact (v Variant(Tuple(UInt64, String))) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part='10G' AS SELECT tuple(42::UInt64, 'str');
        CREATE TABLE test_wide (v Variant(Tuple(UInt64, String))) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part=1, min_rows_for_wide_part=1  AS SELECT tuple(42::UInt64, 'str');
    """
    )

    # Names of variants are persisted in parts: they order the discriminators and name the streams.
    # `interval` and `strict` are quoted when formatting queries, but not in type names.
    node.query("""
        DROP TABLE IF EXISTS test_named_compact;
        DROP TABLE IF EXISTS test_named_wide;
        DROP TABLE IF EXISTS test_named_order;
        DROP TABLE IF EXISTS test_named_dynamic;
        CREATE TABLE test_named_compact (v Variant(String, Tuple(interval UInt64))) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part='10G' AS SELECT CAST(tuple(42), 'Tuple(interval UInt64)')::Variant(String, Tuple(interval UInt64));
        CREATE TABLE test_named_wide (v Variant(String, Tuple(interval UInt64))) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part=1, min_rows_for_wide_part=1 AS SELECT CAST(tuple(42), 'Tuple(interval UInt64)')::Variant(String, Tuple(interval UInt64));
        CREATE TABLE test_named_order (id UInt64, v Variant(String, Tuple(a String), Tuple(interval UInt32))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part=1, min_rows_for_wide_part=1;
        INSERT INTO test_named_order VALUES (1, 'str'), (2, CAST(tuple(10), 'Tuple(interval UInt32)')), (3, CAST(tuple('x'), 'Tuple(a String)'));
        CREATE TABLE test_named_dynamic (d Dynamic) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part=1, min_rows_for_wide_part=1 AS SELECT CAST(tuple(7), 'Tuple(strict UInt64)')::Dynamic;
    """
    )

    node.restart_with_latest_version()

    result = node.query("SELECT v FROM test_compact")
    assert result == "(42,'str')\n"

    result = node.query("SELECT v FROM test_wide")
    assert result == "(42,'str')\n"

    result = node.query("SELECT v.`Tuple(UInt64, String)` FROM test_compact")
    assert result == "(42,'str')\n"

    result = node.query("SELECT v.`Tuple(UInt64, String)` FROM test_wide")
    assert result == "(42,'str')\n"

    for table in ["test_named_compact", "test_named_wide"]:
        assert node.query(f"SELECT v FROM {table}") == "(42)\n"
        assert node.query(f"SELECT v.`Tuple(interval UInt64)` FROM {table}") == "(42)\n"

    order_query = "SELECT id, v, variantType(v), v.`Tuple(a String)`.a FROM test_named_order ORDER BY id"
    order_result = "1\tstr\tString\t\\N\n2\t(10)\tTuple(interval UInt32)\t\\N\n3\t('x')\tTuple(a String)\tx\n"
    assert node.query(order_query) == order_result
    node.query("OPTIMIZE TABLE test_named_order FINAL")
    assert node.query(order_query) == order_result

    assert (
        node.query("SELECT d, dynamicType(d) FROM test_named_dynamic")
        == "(7)\tTuple(strict UInt64)\n"
    )
