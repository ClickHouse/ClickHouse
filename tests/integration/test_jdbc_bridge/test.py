import logging
import os.path as p
import pytest
import uuid

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from string import Template

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance", main_configs=["configs/jdbc_bridge.xml"], with_jdbc_bridge=True
)
datasource = "self"
records = 1000


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        instance.query(
            """
            CREATE DATABASE test;
            CREATE TABLE test.ClickHouseTable(Num UInt32, Str String, Desc Nullable(String)) engine = Memory;
            INSERT INTO test.ClickHouseTable(Num, Str)
            SELECT number, toString(number) FROM system.numbers LIMIT {};
        """.format(
                records
            )
        )

        while True:
            datasources = instance.query("select * from jdbc('', 'show datasources')")
            if "self" in datasources:
                logging.debug(
                    f"JDBC Driver self datasource initialized.\n{datasources}"
                )
                break
            else:
                logging.debug(
                    f"Waiting JDBC Driver to initialize 'self' datasource.\n{datasources}"
                )
        yield cluster
    finally:
        cluster.shutdown()


def test_jdbc_query(started_cluster):
    """Test simple query with inline schema and query parameters"""
    expected = "{}\t{}".format(datasource, records)
    actual = instance.query(
        """
        SELECT * FROM jdbc(
            '{}?datasource_column&fetch_size=1',
            'rows UInt32',
            'SELECT count(1) AS rows FROM test.ClickHouseTable'
        )
    """.format(
            datasource
        )
    )
    assert TSV(actual) == TSV(expected), "expecting {} but got {}".format(
        expected, actual
    )


def test_jdbc_distributed_query(started_cluster):
    """Test distributed query involving both JDBC table function and ClickHouse table"""
    actual = instance.query(
        """
        SELECT a.Num + 1
        FROM jdbc('{0}', 'SELECT * FROM test.ClickHouseTable') a
        INNER JOIN jdbc('{0}', 'num UInt32', 'SELECT {1} - 1 AS num') b
            on a.Num = b.num
        INNER JOIN test.ClickHouseTable c on b.num = c.Num
    """.format(
            datasource, records
        )
    )
    assert int(actual) == records, "expecting {} but got {}".format(records, actual)


def test_jdbc_insert(started_cluster):
    """Test insert query using JDBC table function"""
    instance.query("DROP TABLE IF EXISTS test.test_insert")
    instance.query(
        """
        CREATE TABLE test.test_insert ENGINE = Memory AS
        SELECT * FROM test.ClickHouseTable;
        SELECT * 
        FROM jdbc('{0}?mutation', 'INSERT INTO test.test_insert VALUES({1}, ''{1}'', ''{1}'')');
    """.format(
            datasource, records
        )
    )

    expected = records
    actual = instance.query(
        "SELECT Desc FROM jdbc('{}', 'SELECT * FROM test.test_insert WHERE Num = {}')".format(
            datasource, records
        )
    )
    assert int(actual) == expected, "expecting {} but got {}".format(records, actual)


def test_jdbc_update(started_cluster):
    """Test update query using JDBC table function"""
    secrets = str(uuid.uuid1())
    instance.query("DROP TABLE IF EXISTS test.test_update")
    instance.query(
        """
        CREATE TABLE test.test_update ENGINE = Memory AS
        SELECT * FROM test.ClickHouseTable;
        SELECT * 
        FROM jdbc(
            '{}?mutation',
            'SET mutations_sync = 1; ALTER TABLE test.test_update UPDATE Str=''{}'' WHERE Num = {} - 1;'
        )
    """.format(
            datasource, secrets, records
        )
    )

    actual = instance.query(
        """
        SELECT Str
        FROM jdbc('{}', 'SELECT * FROM test.test_update WHERE Num = {} - 1')
    """.format(
            datasource, records
        )
    )
    assert TSV(actual) == TSV(secrets), "expecting {} but got {}".format(
        secrets, actual
    )


def test_jdbc_delete(started_cluster):
    """Test delete query using JDBC table function"""
    instance.query("DROP TABLE IF EXISTS test.test_delete")
    instance.query(
        """
        CREATE TABLE test.test_delete ENGINE = Memory AS
        SELECT * FROM test.ClickHouseTable;
        SELECT * 
        FROM jdbc(
            '{}?mutation',
            'SET mutations_sync = 1; ALTER TABLE test.test_delete DELETE WHERE Num < {} - 1;'
        )
    """.format(
            datasource, records
        )
    )

    expected = records - 1
    actual = instance.query(
        "SELECT Str FROM jdbc('{}', 'SELECT * FROM test.test_delete')".format(
            datasource, records
        )
    )
    assert int(actual) == expected, "expecting {} but got {}".format(expected, actual)


def test_jdbc_table_engine(started_cluster):
    """Test query against a JDBC table"""
    instance.query("DROP TABLE IF EXISTS test.jdbc_table")
    actual = instance.query(
        """
        CREATE TABLE test.jdbc_table(Str String)
        ENGINE = JDBC('{}', 'test', 'ClickHouseTable');
        SELECT count(1) FROM test.jdbc_table;
    """.format(
            datasource
        )
    )

    assert int(actual) == records, "expecting {} but got {}".format(records, actual)
