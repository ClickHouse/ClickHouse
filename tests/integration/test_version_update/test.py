import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, exec_query_with_retry

cluster = ClickHouseCluster(__file__)


node1 = cluster.add_instance('node1', main_configs=["configs/log_conf.xml"], stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=["configs/log_conf.xml"],
                                      image='yandex/clickhouse-server',
                                      tag='21.5', with_installed_binary=True, stay_alive=True)

node3 = cluster.add_instance('node3', with_zookeeper=True, image='yandex/clickhouse-server', tag='21.2', with_installed_binary=True, stay_alive=True)


def insert_data(node):
    node.query(""" INSERT INTO test_table
                SELECT toDateTime('2020-10-01 19:20:30'), 1,
                sumMapState(arrayMap(i -> 1, range(300)), arrayMap(i -> 1, range(300)));""")


def create_and_fill_table(node):
    node.query("DROP TABLE IF EXISTS test_table;")
    node.query("""
    CREATE TABLE test_table
    (
        `col1` DateTime,
        `col2` Int64,
        `col3` AggregateFunction(sumMap, Array(UInt8), Array(UInt8))
    )
    ENGINE = AggregatingMergeTree() ORDER BY (col1, col2) """)
    insert_data(node)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_modulo_partition_key_issue_23508(start_cluster):
    node3.query("CREATE TABLE test (id Int64, v UInt64, value String) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/table1', '1', v) PARTITION BY id % 20 ORDER BY (id, v)")
    node3.query("INSERT INTO test SELECT number, number, toString(number) FROM numbers(10)")

    expected = node3.query("SELECT number, number, toString(number) FROM numbers(10)")
    partition_data = node3.query("SELECT partition, name FROM system.parts WHERE table='test' ORDER BY partition")
    assert(expected == node3.query("SELECT * FROM test ORDER BY id"))

    node3.restart_with_latest_version()

    assert(expected == node3.query("SELECT * FROM test ORDER BY id"))
    assert(partition_data == node3.query("SELECT partition, name FROM system.parts WHERE table='test' ORDER BY partition"))


def test_aggregate_function_versioning_issue_16587(start_cluster):
    for node in [node1, node2]:
        node.query("DROP TABLE IF EXISTS test_table;")
        node.query("""
        CREATE TABLE test_table (`col1` DateTime, `col2` Int64)
        ENGINE = MergeTree() ORDER BY col1""")
        node.query("insert into test_table select '2020-10-26 00:00:00', 70724110 from numbers(300)")

    expected = "([1],[600])"

    # Incorrect result on old server
    result_on_old_version = node2.query("select sumMap(sm) from (select sumMap([1],[1]) as sm from remote('127.0.0.{1,2}', default.test_table) group by col1, col2);")
    assert(result_on_old_version.strip() != expected)

    # Correct result on new server
    result_on_new_version = node1.query("select sumMap(sm) from (select sumMap([1],[1]) as sm from remote('127.0.0.{1,2}', default.test_table) group by col1, col2);")
    assert(result_on_new_version.strip() == expected)


def test_aggregate_function_versioning_fetch_data_from_new_to_old_server(start_cluster):
    for node in [node1, node2]:
        create_and_fill_table(node)

    expected = "([1],[300])"

    new_server_data = node1.query("select finalizeAggregation(col3) from default.test_table;").strip()
    assert(new_server_data == expected)

    old_server_data = node2.query("select finalizeAggregation(col3) from default.test_table;").strip()
    assert(old_server_data != expected)

    data_from_old_to_new_server = node1.query("select finalizeAggregation(col3) from remote('node2', default.test_table);").strip()
    assert(data_from_old_to_new_server == old_server_data)


def test_aggregate_function_versioning_server_upgrade(start_cluster):
    for node in [node1, node2]:
        create_and_fill_table(node)

    new_server_data = node1.query("select finalizeAggregation(col3) from default.test_table;").strip()
    assert(new_server_data == "([1],[300])")
    old_server_data = node2.query("select finalizeAggregation(col3) from default.test_table;").strip()
    assert(old_server_data == "([1],[44])")

    node2.restart_with_latest_version()

    # Check that after server upgrade aggregate function is serialized according to older version.
    upgraded_server_data = node2.query("select finalizeAggregation(col3) from default.test_table;").strip()
    assert(upgraded_server_data == "([1],[44])")

    # Remote fetches are still with older version.
    data_from_upgraded_to_new_server = node1.query("select finalizeAggregation(col3) from remote('node2', default.test_table);").strip()
    assert(data_from_upgraded_to_new_server == upgraded_server_data == "([1],[44])")

    # Check it is ok to write into table with older version of aggregate function.
    insert_data(node2)

    # Hm, should newly inserted data be serialized as old version?
    upgraded_server_data = node2.query("select finalizeAggregation(col3) from default.test_table;").strip()
    assert(upgraded_server_data == "([1],[300])\n([1],[44])")


def test_aggregate_function_versioning_persisting_metadata(start_cluster):
    for node in [node1, node2]:
        create_and_fill_table(node)
    node2.restart_with_latest_version()

    for node in [node1, node2]:
        node.query("DETACH TABLE test_table")
        node.query("ATTACH TABLE test_table")

    for node in [node1, node2]:
        insert_data(node)

    new_server_data = node1.query("select finalizeAggregation(col3) from default.test_table;").strip()
    assert(new_server_data == "([1],[300])\n([1],[300])")
    upgraded_server_data = node2.query("select finalizeAggregation(col3) from default.test_table;").strip()
    assert(upgraded_server_data == "([1],[44])\n([1],[44])")

    for node in [node1, node2]:
        node.restart_clickhouse()
        insert_data(node)

    result = node1.query("select finalizeAggregation(col3) from remote('127.0.0.{1,2}', default.test_table);").strip()
    assert(result == "([1],[300])\n([1],[300])\n([1],[300])\n([1],[300])")
    result = node2.query("select finalizeAggregation(col3) from remote('127.0.0.{1,2}', default.test_table);").strip()
    assert(result == "([1],[44])\n([1],[44])\n([1],[44])\n([1],[44])")

