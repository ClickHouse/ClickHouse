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

    node.restart_with_latest_version()

    result = node.query("SELECT v FROM test_compact")
    assert result == "(42,'str')\n"

    result = node.query("SELECT v FROM test_wide")
    assert result == "(42,'str')\n"

    result = node.query("SELECT v.`Tuple(UInt64, String)` FROM test_compact")
    assert result == "(42,'str')\n"

    result = node.query("SELECT v.`Tuple(UInt64, String)` FROM test_wide")
    assert result == "(42,'str')\n"
