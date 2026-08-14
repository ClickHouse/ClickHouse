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
        instance.query("DROP DATABASE IF EXISTS test_db")
        instance.query("DROP DATABASE IF EXISTS test_user_db")
        instance.query("DROP DATABASE IF EXISTS test_fs_db")


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


def test_create_database_does_not_require_table_engine_grant():
    # Regression test for PR #98984: `CREATE DATABASE` must not require `TABLE ENGINE` grants
    # even when `table_engines_require_grant = false`. Database engines (e.g. `Atomic`) are
    # registered in `DatabaseFactory`, not `StorageFactory`. Before the fix, only
    # `StorageFactory::getAllStorages()` was iterated for implicit grants, omitting database
    # engines and causing `ACCESS_DENIED` on `CREATE DATABASE`.
    instance.query("DROP USER IF EXISTS A")
    instance.query("CREATE USER A")
    instance.query("GRANT CREATE DATABASE ON *.* TO A")

    # `CREATE DATABASE` with the default engine must succeed.
    instance.query("CREATE DATABASE test_user_db", user="A")
    instance.query("DROP DATABASE test_user_db")

    # Explicitly specifying `Atomic` must also succeed — it is a database engine, not a table engine.
    instance.query("CREATE DATABASE test_user_db ENGINE = Atomic", user="A")
    instance.query("DROP DATABASE test_user_db")


def test_database_source_engine_requires_source_grant():
    # Database engines that connect to external sources declare source_access_type in
    # DatabaseFactory, just like table engines do in StorageFactory. Even when
    # table_engines_require_grant = false, source database engines still require the
    # corresponding READ/WRITE source grant.
    instance.query("DROP USER IF EXISTS A")
    instance.query("CREATE USER A")
    instance.query("GRANT CREATE DATABASE ON *.* TO A")

    # Filesystem database engine requires FILE source access.
    assert "Not enough privileges" in instance.query_and_get_error(
        "CREATE DATABASE test_fs_db ENGINE = Filesystem",
        user="A",
    )

    instance.query("GRANT READ, WRITE ON FILE TO A")

    instance.query("CREATE DATABASE test_fs_db ENGINE = Filesystem", user="A")
    instance.query("DROP DATABASE test_fs_db")

    # Revoking FILE source access blocks Filesystem database creation again.
    instance.query("REVOKE READ, WRITE ON FILE FROM A")

    assert "Not enough privileges" in instance.query_and_get_error(
        "CREATE DATABASE test_fs_db ENGINE = Filesystem",
        user="A",
    )
