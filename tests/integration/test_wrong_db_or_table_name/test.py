import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def start():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_wrong_database_name(start):
    node.query(
        """
        CREATE DATABASE test;
        CREATE TABLE test.table_test (i Int64) ENGINE=Memory;
        INSERT INTO test.table_test SELECT 1;
        """
    )

    with pytest.raises(
        QueryRuntimeException,
        match="DB::Exception: Database tes does not exist. Maybe you meant test?.",
    ):
        node.query("SELECT * FROM tes.table_test LIMIT 1;")
    assert int(node.query("SELECT count() FROM test.table_test;")) == 1
    node.query(
        """
        DROP TABLE test.table_test;
        DROP DATABASE test;
        """
    )


def test_drop_wrong_database_name(start):
    node.query(
        """
        CREATE DATABASE test;
        CREATE TABLE test.table_test (i Int64) ENGINE=Memory;
        INSERT INTO test.table_test SELECT 1;
        """
    )

    with pytest.raises(
        QueryRuntimeException,
        match="DB::Exception: Database tes does not exist. Maybe you meant test?.",
    ):
        node.query("DROP DATABASE tes;")
    assert int(node.query("SELECT count() FROM test.table_test;")) == 1
    node.query("DROP DATABASE test;")


def test_database_engine_name(start):
    # test with a valid database engine
    node.query(
        """
        CREATE DATABASE test_atomic ENGINE = Atomic;
        CREATE TABLE test_atomic.table_test_atomic (i Int64) ENGINE = MergeTree() ORDER BY i;
        INSERT INTO test_atomic.table_test_atomic SELECT 1;
        """
    )
    assert 1 == int(node.query("SELECT * FROM test_atomic.table_test_atomic".strip()))
    # test with a invalid database engine
    with pytest.raises(
        QueryRuntimeException,
        match="DB::Exception: Unknown database engine Atomic123. Maybe you meant: \\['Atomic'\\].",
    ):
        node.query("CREATE DATABASE test_atomic123 ENGINE = Atomic123;")

    node.query(
        """
        DROP TABLE test_atomic.table_test_atomic;
        DROP DATABASE test_atomic;
       """
    )


def test_wrong_table_name(start):
    node.query(
        """
        CREATE DATABASE test;
        CREATE DATABASE test2;
        CREATE TABLE test.table_test (i Int64) ENGINE=Memory;
        CREATE TABLE test.table_test2 (i Int64) ENGINE=Memory;
        INSERT INTO test.table_test SELECT 1;
        """
    )

    error_message = node.query_and_get_error(
        """
            SELECT * FROM test.table_test1 LIMIT 1;
            """
    )
    assert (
        "DB::Exception: Table test.table_test1 does not exist. Maybe you meant test.table_test?"
        in error_message
        or "DB::Exception: Unknown table expression identifier 'test.table_test1' in scope SELECT * FROM test.table_test1 LIMIT 1."
        in error_message
    )
    assert int(node.query("SELECT count() FROM test.table_test;")) == 1

    error_message = node.query_and_get_error(
        """
            SELECT * FROM test2.table_test1 LIMIT 1;
            """
    )
    assert (
        "DB::Exception: Table test2.table_test1 does not exist. Maybe you meant test.table_test?."
        in error_message
        or "DB::Exception: Unknown table expression identifier 'test2.table_test1' in scope SELECT * FROM test2.table_test1 LIMIT 1."
        in error_message
    )

    node.query(
        """
            DROP TABLE test.table_test;
            DROP TABLE test.table_test2;
            DROP DATABASE test;
            DROP DATABASE test2;
            """
    )


def test_drop_wrong_table_name(start):
    node.query(
        """
        CREATE DATABASE test;
        CREATE DATABASE test2;
        CREATE TABLE test.table_test (i Int64) ENGINE=Memory;
        INSERT INTO test.table_test SELECT 1;
        """
    )

    with pytest.raises(
        QueryRuntimeException,
        match="DB::Exception: Table test.table_test1 does not exist. Maybe you meant test.table_test?.",
    ):
        node.query("DROP TABLE test.table_test1;")
    assert int(node.query("SELECT count() FROM test.table_test;")) == 1

    with pytest.raises(
        QueryRuntimeException,
        match="DB::Exception: Table test2.table_test does not exist. Maybe you meant test.table_test?.",
    ):
        node.query("DROP TABLE test2.table_test;")

    node.query(
        """
        DROP TABLE test.table_test;
        DROP DATABASE test;
        DROP DATABASE test2;
        """
    )


def test_wrong_database_name_hint(start):
    node.query(
        """
        CREATE DATABASE test_db_visible;
        CREATE DATABASE test_db_hidden;

        CREATE TABLE test_db_visible.depth (id UInt32, value String) ENGINE = Memory;
        CREATE TABLE test_db_hidden.depth (id UInt32, secret_data String) ENGINE = Memory;

        INSERT INTO test_db_visible.depth VALUES (1, 'visible data');
        INSERT INTO test_db_hidden.depth VALUES (1, 'hidden secret');

        CREATE USER restricted_user;

        GRANT SELECT ON test_db_visible.* TO restricted_user;
        REVOKE ALL ON test_db_hidden.* FROM restricted_user;
        """
    )

    error_message = node.query_and_get_error("SELECT * FROM test_db_hidde.depth;", user="restricted_user")
    assert (
        "test_db_hidden"
        not in error_message
    )

    node.query(
        """
        DROP DATABASE IF EXISTS test_db_visible;
        DROP DATABASE IF EXISTS test_db_hidden;

        DROP USER IF EXISTS restricted_user;
        """
    )
