# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node", main_configs=["configs/rocksdb.xml"], stay_alive=True
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_read_only(start_cluster):
    # fail if read_only = true and directory does not exist.
    with pytest.raises(QueryRuntimeException):
        node.query(
            """
        CREATE TABLE test (key UInt64, value String) Engine=EmbeddedRocksDB(0, '/var/lib/clickhouse/user_files/test_rocksdb_read_only', 1) PRIMARY KEY(key);
        """
        )
    # create directory if read_only = false
    node.query(
        """
    CREATE TABLE test (key UInt64, value String) Engine=EmbeddedRocksDB(0, '/var/lib/clickhouse/user_files/test_rocksdb_read_only') PRIMARY KEY(key);
    INSERT INTO test (key, value) VALUES (0, 'a'), (1, 'b'), (2, 'c');
    """
    )
    # fail if create multiple non-read-only tables on the same directory
    with pytest.raises(QueryRuntimeException):
        node.query(
            """
        CREATE TABLE test_fail (key UInt64, value String) Engine=EmbeddedRocksDB(0, '/var/lib/clickhouse/user_files/test_rocksdb_read_only') PRIMARY KEY(key);
        """
        )
    with pytest.raises(QueryRuntimeException):
        node.query(
            """
        CREATE TABLE test_fail (key UInt64, value String) Engine=EmbeddedRocksDB(10, '/var/lib/clickhouse/user_files/test_rocksdb_read_only') PRIMARY KEY(key);
        """
        )
    # success if create multiple read-only tables on the same directory
    node.query(
        """
    CREATE TABLE test_1 (key UInt64, value String) Engine=EmbeddedRocksDB(0, '/var/lib/clickhouse/user_files/test_rocksdb_read_only', 1) PRIMARY KEY(key);
    DROP TABLE test_1;
    """
    )
    node.query(
        """
    CREATE TABLE test_2 (key UInt64, value String) Engine=EmbeddedRocksDB(10, '/var/lib/clickhouse/user_files/test_rocksdb_read_only', 1) PRIMARY KEY(key);
    DROP TABLE test_2;
    """
    )
    # success if create table on existing directory with no other tables on it
    node.query(
        """
    DROP TABLE test;
    CREATE TABLE test (key UInt64, value String) Engine=EmbeddedRocksDB(10, '/var/lib/clickhouse/user_files/test_rocksdb_read_only', 1) PRIMARY KEY(key);
    """
    )
    result = node.query("""SELECT count() FROM test;""")
    assert result.strip() == "3"
    # fail if insert into table with read_only = true
    with pytest.raises(QueryRuntimeException):
        node.query(
            """INSERT INTO test (key, value) VALUES (4, 'd');
        """
        )
    node.query(
        """
    DROP TABLE test;
    """
    )


def test_dirctory_missing_after_stop(start_cluster):
    # for read_only = false
    node.query(
        """
    CREATE TABLE test_missing (key UInt64, value String) Engine=EmbeddedRocksDB(0, '/var/lib/clickhouse/user_files/test_rocksdb_read_only_missing') PRIMARY KEY(key);
    """
    )
    node.stop_clickhouse()
    node.exec_in_container(
        [
            "bash",
            "-c",
            "rm -r /var/lib/clickhouse/user_files/test_rocksdb_read_only_missing",
        ]
    )
    node.start_clickhouse()
    result = node.query(
        """INSERT INTO test_missing (key, value) VALUES (0, 'a');
    SELECT * FROM test_missing;
    """
    )
    assert result.strip() == "0\ta"
    node.query(
        """DROP TABLE test_missing;
    """
    )
    # for read_only = true
    node.query(
        """
    CREATE TABLE test_missing (key UInt64, value String) Engine=EmbeddedRocksDB(0, '/var/lib/clickhouse/user_files/test_rocksdb_read_only_missing', 1) PRIMARY KEY(key);
    """
    )
    node.stop_clickhouse()
    node.exec_in_container(
        [
            "bash",
            "-c",
            "rm -r /var/lib/clickhouse/user_files/test_rocksdb_read_only_missing",
        ]
    )
    node.start_clickhouse()
    with pytest.raises(QueryRuntimeException):
        node.query("""INSERT INTO test_missing (key, value) VALUES (1, 'b');""")
    result = node.query("""SELECT * FROM test_missing;""")
    assert result.strip() == ""
    node.query(
        """DROP TABLE test_missing;
    """
    )
