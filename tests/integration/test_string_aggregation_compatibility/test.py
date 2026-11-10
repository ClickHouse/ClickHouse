import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1")
node2 = cluster.add_instance("node2")
node256 = cluster.add_instance(
    "node256",
    image="clickhouse/clickhouse-server",
    tag="25.6",
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_string_aggregation_compatibility(started_cluster):
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

    create_tables(node1, other_node_name=node256.name)
    create_tables(node256, other_node_name=node1.name)

    def run_query(node, extra_settings={}):
        return int(
            node.query(
                """
        SELECT count()
        FROM
        (
            SELECT
                id,
                s2
            FROM global_repro1
            GROUP BY ALL
        )""",
                settings={"group_by_two_level_threshold": 1} | extra_settings,
            )
        )

    assert run_query(node1) == 10000
    assert run_query(node256) == 10000
    assert (
        run_query(node1, extra_settings={"serialize_string_in_memory_with_zero_byte": 0})
        == 20000
    )

def test_string_aggregation_compatibility_setting(started_cluster):
    def create_tables(n, other_node_name):
        n.query(
            "DROP TABLE IF EXISTS repro2 SYNC; CREATE TABLE repro2 (id UInt64, s1 String, s2 LowCardinality(String)) ENGINE = MergeTree ORDER BY id"
        )
        n.query(
            "INSERT INTO repro2 SELECT number, 'somestring', 'somestring' FROM numbers(10000)"
        )
        n.query(
            f"CREATE TABLE IF NOT EXISTS dist_repro2 (id UInt64, s1 String, s2 LowCardinality(String)) AS remote('{other_node_name}', 'default.repro2', 'default', '')"
        )
        n.query(
            "CREATE TABLE IF NOT EXISTS global_repro2 (id UInt64, s1 String, s2 LowCardinality(String)) ENGINE = Merge('default', '.*repro2')"
        )

    create_tables(node1, other_node_name=node2.name)
    create_tables(node2, other_node_name=node1.name)

    def run_query(n, extra_settings={}):
        return int(
            n.query(
                """
        SELECT count()
        FROM
        (
            SELECT
                id,
                s2
            FROM global_repro2
            GROUP BY ALL
        )""",
                settings={"group_by_two_level_threshold": 1} | extra_settings,
            )
        )

    assert run_query(node1) == 10000
    assert run_query(node2) == 10000
    assert (
        run_query(node1, extra_settings={"serialize_string_in_memory_with_zero_byte": 0})
        == 10000
    )
