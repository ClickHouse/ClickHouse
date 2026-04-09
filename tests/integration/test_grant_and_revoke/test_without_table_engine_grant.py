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
        instance.query("DROP USER IF EXISTS A, B")
        instance.query("DROP TABLE IF EXISTS test.table1")


def test_table_engine_and_source_grant():
    instance.query("DROP USER IF EXISTS A")
    instance.query("CREATE USER A")
    instance.query("GRANT CREATE TABLE ON test.table1 TO A")

    instance.query("GRANT READ, WRITE ON POSTGRES TO A")

    instance.query(
        """
        CREATE TABLE test.table1(a Integer)
        engine=PostgreSQL('localhost:5432', 'dummy', 'dummy', 'dummy', 'dummy');
        """,
        user="A",
    )

    instance.query("DROP TABLE test.table1")

    instance.query("REVOKE READ, WRITE ON POSTGRES FROM A")

    assert "Not enough privileges" in instance.query_and_get_error(
        """
        CREATE TABLE test.table1(a Integer)
        engine=PostgreSQL('localhost:5432', 'dummy', 'dummy', 'dummy', 'dummy');
        """,
        user="A",
    )

    # expecting grant POSTGRES instead of grant PostgreSQL due to discrepancy between source access type and table engine
    # similarily, other sources should also use their own defined name instead of the name of table engine
    assert "grant TABLE ENGINE ON PostgreSQL" in instance.query_and_get_error(
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


def test_source_revocation_blocks_table_engine():
    instance.query("DROP USER IF EXISTS A")
    instance.query("CREATE USER A")
    instance.query("GRANT CREATE TABLE ON test.table1 TO A")

    assert "Not enough privileges" in instance.query_and_get_error(
        "CREATE TABLE test.table1(a Integer) engine=URL('http://localhost:65535/dummy', 'CSV')",
        user="A",
    )

    instance.query(
        "CREATE TABLE test.table1(a Integer) engine=TinyLog",
        user="A",
    )
    instance.query("DROP TABLE test.table1")

    instance.query("GRANT READ, WRITE ON URL TO A")

    instance.query(
        "CREATE TABLE test.table1(a Integer) engine=URL('http://localhost:65535/dummy', 'CSV')",
        user="A",
    )

    instance.query("DROP TABLE test.table1")

    instance.query("REVOKE READ, WRITE ON URL FROM A")

    assert "Not enough privileges" in instance.query_and_get_error(
        "CREATE TABLE test.table1(a Integer) engine=URL('http://localhost:65535/dummy', 'CSV')",
        user="A",
    )


def test_grant_table_engine_option():
    instance.query("DROP USER IF EXISTS A, B")
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")
    instance.query("GRANT TABLE ENGINE ON * TO A WITH GRANT OPTION")

    instance.query("GRANT TABLE ENGINE ON * TO B", user="A")

    assert "GRANT TABLE ENGINE ON * TO B" in instance.query("SHOW GRANTS FOR B")

    instance.query("DROP USER IF EXISTS B")
