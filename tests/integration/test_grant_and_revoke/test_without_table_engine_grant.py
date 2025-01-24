import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/config_without_table_engine_grant.xml"],
    user_configs=["configs/users.d/users.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()

        instance.query("CREATE DATABASE test")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        instance.query("DROP USER IF EXISTS A")
        instance.query("DROP TABLE IF EXISTS test.table1")


def test_table_engine_and_source_grant():
    instance.query("DROP USER IF EXISTS A")
    instance.query("CREATE USER A")
    instance.query("GRANT CREATE TABLE ON test.table1 TO A")

    instance.query("GRANT POSTGRES ON *.* TO A")

    instance.query(
        """
        CREATE TABLE test.table1(a Integer)
        engine=PostgreSQL('localhost:5432', 'dummy', 'dummy', 'dummy', 'dummy');
        """,
        user="A",
    )

    instance.query("DROP TABLE test.table1")

    instance.query("REVOKE POSTGRES ON *.* FROM A")

    assert "Not enough privileges" in instance.query_and_get_error(
        """
        CREATE TABLE test.table1(a Integer)
        engine=PostgreSQL('localhost:5432', 'dummy', 'dummy', 'dummy', 'dummy');
        """,
        user="A",
    )

    # expecting grant POSTGRES instead of grant PostgreSQL due to discrepancy between source access type and table engine
    # similarily, other sources should also use their own defined name instead of the name of table engine
    assert "grant POSTGRES ON *.*" in instance.query_and_get_error(
        """
        CREATE TABLE test.table1(a Integer)
        engine=PostgreSQL('localhost:5432', 'dummy', 'dummy', 'dummy', 'dummy');
        """,
        user="A",
    )

    instance.query("GRANT SOURCES ON *.* TO A")

    instance.query(
        """
        CREATE TABLE test.table1(a Integer)
        engine=PostgreSQL('localhost:5432', 'dummy', 'dummy', 'dummy', 'dummy');
        """,
        user="A",
    )

    instance.query("DROP TABLE test.table1")
