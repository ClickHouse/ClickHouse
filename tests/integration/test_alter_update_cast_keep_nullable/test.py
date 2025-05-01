import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", user_configs=["configs/users.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_cast_keep_nullable(started_cluster):
    setting = node1.query(
        "SELECT value FROM system.settings WHERE name='cast_keep_nullable'"
    )
    assert setting.strip() == "1"

    result = node1.query(
        """
        DROP TABLE IF EXISTS t;
        CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY tuple();
        INSERT INTO t SELECT number FROM numbers(10);
        SELECT * FROM t;
    """
    )
    assert result.strip() == "0\n1\n2\n3\n4\n5\n6\n7\n8\n9"

    error = node1.query_and_get_error(
        """
        SET mutations_sync = 1;
        ALTER TABLE t UPDATE x = x % 3 = 0 ? NULL : x WHERE x % 2 = 1;　
    """
    )
    assert "DB::Exception: Cannot convert NULL value to non-Nullable type" in error

    result = node1.query("SELECT * FROM t;")
    assert result.strip() == "0\n1\n2\n3\n4\n5\n6\n7\n8\n9"
