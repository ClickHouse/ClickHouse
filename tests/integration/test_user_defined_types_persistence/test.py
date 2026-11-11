import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_persistence():
    instance.query("CREATE TYPE MyId AS UInt64")
    instance.query("CREATE TYPE MyPair(K, V) AS Tuple(K, V)")
    instance.query("CREATE TYPE MyRecord AS Tuple(MyId, MyPair(String, Float64))")

    expected_type = "Tuple(UInt64, Tuple(String, Float64))\n"
    assert (
        instance.query("SELECT toTypeName(CAST((1, ('a', 1.5)), 'MyRecord'))")
        == expected_type
    )

    # The definitions are stored as `CREATE TYPE` queries next to the user-defined functions.
    files = instance.exec_in_container(
        ["bash", "-c", "ls /var/lib/clickhouse/user_defined/"]
    ).split()
    assert sorted(files) == ["type_MyId.sql", "type_MyPair.sql", "type_MyRecord.sql"]
    assert (
        instance.exec_in_container(
            ["cat", "/var/lib/clickhouse/user_defined/type_MyPair.sql"]
        )
        == "CREATE TYPE MyPair(K, V) AS Tuple(K, V)\n"
    )

    # A table expression can refer to a type by name, so the types are loaded before the tables.
    instance.query(
        "CREATE TABLE key_table (x UInt8) ENGINE = MergeTree ORDER BY CAST(x, 'MyId')"
    )
    instance.query("INSERT INTO key_table VALUES (2), (10)")

    instance.restart_clickhouse()

    assert instance.query("SHOW TYPES") == "MyId\nMyPair\nMyRecord\n"
    assert (
        instance.query("SELECT x FROM key_table WHERE CAST(x, 'MyId') = 10") == "10\n"
    )
    instance.query("DROP TABLE key_table SYNC")
    assert (
        instance.query("SELECT toTypeName(CAST((1, ('a', 1.5)), 'MyRecord'))")
        == expected_type
    )
    assert (
        instance.query(
            "SELECT create_query FROM system.user_defined_types WHERE name = 'MyPair'"
        )
        == "CREATE TYPE MyPair(K, V) AS Tuple(K, V)\n"
    )

    # Dependencies between types are derived from the stored definitions, so they survive the restart too.
    assert "Cannot drop user-defined type `MyId`" in instance.query_and_get_error(
        "DROP TYPE MyId"
    )

    instance.query("DROP TYPE MyRecord")
    instance.query("DROP TYPE MyPair")
    instance.query("DROP TYPE MyId")

    instance.restart_clickhouse()

    assert instance.query("SHOW TYPES") == ""
    assert "Unknown data type family: MyId" in instance.query_and_get_error(
        "SELECT CAST(1, 'MyId')"
    )
