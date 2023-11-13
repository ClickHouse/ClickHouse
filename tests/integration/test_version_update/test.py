import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, exec_query_with_retry

cluster = ClickHouseCluster(__file__)


node1 = cluster.add_instance("node1", stay_alive=True)

node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    image="yandex/clickhouse-server",
    tag="21.2",
    with_installed_binary=True,
    stay_alive=True,
)

# Use differents nodes because if there is node.restart_from_latest_version(), then in later tests
# it will be with latest version, but shouldn't, order of tests in CI is shuffled.
node3 = cluster.add_instance(
    "node3",
    image="yandex/clickhouse-server",
    tag="21.5",
    with_installed_binary=True,
    stay_alive=True,
)
node4 = cluster.add_instance(
    "node4",
    image="yandex/clickhouse-server",
    tag="21.5",
    with_installed_binary=True,
    stay_alive=True,
)
node5 = cluster.add_instance(
    "node5",
    image="yandex/clickhouse-server",
    tag="21.5",
    with_installed_binary=True,
    stay_alive=True,
)
node6 = cluster.add_instance(
    "node6",
    image="yandex/clickhouse-server",
    tag="21.5",
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


# Test from issue 16587
def test_aggregate_function_versioning_issue_16587(start_cluster):
    for node in [node1, node3]:
        node.query("DROP TABLE IF EXISTS test_table;")
        node.query(
            """
        CREATE TABLE test_table (`col1` DateTime, `col2` Int64)
        ENGINE = MergeTree() ORDER BY col1"""
        )
        node.query(
            "insert into test_table select '2020-10-26 00:00:00', 1929292 from numbers(300)"
        )

    expected = "([1],[600])"

    result_on_old_version = node3.query(
        "select sumMap(sm) from (select sumMap([1],[1]) as sm from remote('127.0.0.{1,2}', default.test_table) group by col1, col2);"
    ).strip()
    assert result_on_old_version != expected

    result_on_new_version = node1.query(
        "select sumMap(sm) from (select sumMap([1],[1]) as sm from remote('127.0.0.{1,2}', default.test_table) group by col1, col2);"
    ).strip()
    assert result_on_new_version == expected


def test_aggregate_function_versioning_fetch_data_from_old_to_new_server(start_cluster):
    for node in [node1, node4]:
        create_table(node)
        insert_data(node)

    expected = "([1],[300])"

    new_server_data = node1.query(
        "select finalizeAggregation(col3) from default.test_table;"
    ).strip()
    assert new_server_data == expected

    old_server_data = node4.query(
        "select finalizeAggregation(col3) from default.test_table;"
    ).strip()
    assert old_server_data != expected

    data_from_old_to_new_server = node1.query(
        "select finalizeAggregation(col3) from remote('node4', default.test_table);"
    ).strip()
    assert data_from_old_to_new_server == old_server_data


def test_aggregate_function_versioning_server_upgrade(start_cluster):
    for node in [node1, node5]:
        create_table(node)
    insert_data(node1, col2=5)
    insert_data(node5, col2=1)

    # Serialization with version 0, server does not support versioning of aggregate function states.
    old_server_data = node5.query(
        "select finalizeAggregation(col3) from default.test_table;"
    ).strip()
    assert old_server_data == "([1],[44])"
    create = node5.query("describe table default.test_table;").strip()
    assert create.strip().endswith(
        "col3\tAggregateFunction(sumMap, Array(UInt8), Array(UInt8))"
    )
    print("Ok 1")

    # Upgrade server.
    node5.restart_with_latest_version()

    # Deserialized with version 0, server supports versioning.
    upgraded_server_data = node5.query(
        "select finalizeAggregation(col3) from default.test_table;"
    ).strip()
    assert upgraded_server_data == "([1],[44])"
    create = node5.query("describe table default.test_table;").strip()
    assert create.strip().endswith(
        "col3\tAggregateFunction(sumMap, Array(UInt8), Array(UInt8))"
    )
    print("Ok 2")

    create = node1.query("describe table default.test_table;").strip()
    print(create)
    assert create.strip().endswith(
        "col3\tAggregateFunction(1, sumMap, Array(UInt8), Array(UInt8))"
    )

    # Data from upgraded server to new server. Deserialize with version 0.
    data_from_upgraded_to_new_server = node1.query(
        "select finalizeAggregation(col3) from remote('node5', default.test_table);"
    ).strip()
    assert data_from_upgraded_to_new_server == upgraded_server_data == "([1],[44])"
    print("Ok 3")

    # Data is serialized according to version 0 (though one of the states is version 1, but result is version 0).
    upgraded_server_data = node5.query(
        "select finalizeAggregation(col3) from remote('127.0.0.{1,2}', default.test_table);"
    ).strip()
    assert upgraded_server_data == "([1],[44])\n([1],[44])"
    print("Ok 4")

    # Check insertion after server upgarde.
    insert_data(node5, col2=2)

    # Check newly inserted data is still serialized with 0 version.
    upgraded_server_data = node5.query(
        "select finalizeAggregation(col3) from default.test_table order by col2;"
    ).strip()
    assert upgraded_server_data == "([1],[44])\n([1],[44])"
    print("Ok 5")

    # New table has latest version.
    new_server_data = node1.query(
        "select finalizeAggregation(col3) from default.test_table;"
    ).strip()
    assert new_server_data == "([1],[300])"
    print("Ok 6")

    # Insert from new server (with version 1) to upgraded server (where version will be 0), result version 0.
    node1.query(
        "insert into table function remote('node5', default.test_table) select * from default.test_table;"
    ).strip()
    upgraded_server_data = node5.query(
        "select finalizeAggregation(col3) from default.test_table order by col2;"
    ).strip()
    assert upgraded_server_data == "([1],[44])\n([1],[44])\n([1],[44])"
    print("Ok 7")

    # But new table gets data with latest version.
    insert_data(node1)
    new_server_data = node1.query(
        "select finalizeAggregation(col3) from default.test_table;"
    ).strip()
    assert new_server_data == "([1],[300])\n([1],[300])"
    print("Ok 8")

    # Create table with column implicitly with older version (version 0).
    create_table(node1, name="test_table_0", version=0)
    insert_data(node1, table_name="test_table_0", col2=3)
    data = node1.query(
        "select finalizeAggregation(col3) from default.test_table_0;"
    ).strip()
    assert data == "([1],[44])"
    print("Ok")

    # Insert from new server to upgraded server to a new table but the version was set implicitly to 0, so data version 0.
    node1.query(
        "insert into table function remote('node5', default.test_table) select * from default.test_table_0;"
    ).strip()
    upgraded_server_data = node5.query(
        "select finalizeAggregation(col3) from default.test_table order by col2;"
    ).strip()
    assert upgraded_server_data == "([1],[44])\n([1],[44])\n([1],[44])\n([1],[44])"
    print("Ok")


def test_aggregate_function_versioning_persisting_metadata(start_cluster):
    for node in [node1, node6]:
        create_table(node)
        insert_data(node)
    data = node1.query(
        "select finalizeAggregation(col3) from default.test_table;"
    ).strip()
    assert data == "([1],[300])"
    data = node6.query(
        "select finalizeAggregation(col3) from default.test_table;"
    ).strip()
    assert data == "([1],[44])"

    node6.restart_with_latest_version()

    for node in [node1, node6]:
        node.query("DETACH TABLE test_table")
        node.query("ATTACH TABLE test_table")

    for node in [node1, node6]:
        insert_data(node)

    new_server_data = node1.query(
        "select finalizeAggregation(col3) from default.test_table;"
    ).strip()
    assert new_server_data == "([1],[300])\n([1],[300])"

    upgraded_server_data = node6.query(
        "select finalizeAggregation(col3) from default.test_table;"
    ).strip()
    assert upgraded_server_data == "([1],[44])\n([1],[44])"

    for node in [node1, node6]:
        node.restart_clickhouse()
        insert_data(node)

    result = node1.query(
        "select finalizeAggregation(col3) from remote('127.0.0.{1,2}', default.test_table);"
    ).strip()
    assert (
        result
        == "([1],[300])\n([1],[300])\n([1],[300])\n([1],[300])\n([1],[300])\n([1],[300])"
    )

    result = node6.query(
        "select finalizeAggregation(col3) from remote('127.0.0.{1,2}', default.test_table);"
    ).strip()
    assert (
        result
        == "([1],[44])\n([1],[44])\n([1],[44])\n([1],[44])\n([1],[44])\n([1],[44])"
    )
