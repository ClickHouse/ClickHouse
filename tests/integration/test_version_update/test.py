import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)


node1 = cluster.add_instance("node1", stay_alive=True)

node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    with_installed_binary=True,
    stay_alive=True,
)


def insert_data(node, table_name="test_table", n=1, col2=1):
    node.query(
        """ INSERT INTO {}
                SELECT toDateTime(NOW()), {},
                sumMapState(arrayMap(i -> 1, range(300)), arrayMap(i -> 1, range(300)))
                FROM numbers({});""".format(
            table_name, col2, n
        )
    )


def create_table(node, name="test_table", version=None):
    node.query("DROP TABLE IF EXISTS {};".format(name))
    if version is None:
        node.query(
            """
        CREATE TABLE {}
        (
            `col1` DateTime,
            `col2` Int64,
            `col3` AggregateFunction(sumMap, Array(UInt8), Array(UInt8))
        )
        ENGINE = AggregatingMergeTree() ORDER BY (col1, col2) """.format(
                name
            )
        )
    else:
        node.query(
            """
        CREATE TABLE {}
        (
            `col1` DateTime,
            `col2` Int64,
            `col3` AggregateFunction({}, sumMap, Array(UInt8), Array(UInt8))
        )
        ENGINE = AggregatingMergeTree() ORDER BY (col1, col2) """.format(
                name, version
            )
        )


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_modulo_partition_key_issue_23508(start_cluster):
    node2.query(
        "CREATE TABLE test (id Int64, v UInt64, value String) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/table1', '1', v) PARTITION BY id % 20 ORDER BY (id, v)"
    )
    node2.query(
        "INSERT INTO test SELECT number, number, toString(number) FROM numbers(10)"
    )

    expected = node2.query("SELECT number, number, toString(number) FROM numbers(10)")
    partition_data = node2.query(
        "SELECT partition, name FROM system.parts WHERE table='test' ORDER BY partition"
    )
    assert expected == node2.query("SELECT * FROM test ORDER BY id")

    node2.restart_with_latest_version()

    assert expected == node2.query("SELECT * FROM test ORDER BY id")
    assert partition_data == node2.query(
        "SELECT partition, name FROM system.parts WHERE table='test' ORDER BY partition"
    )
