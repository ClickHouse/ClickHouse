import pytest, time
from helpers.cluster import ClickHouseCluster
cluster = ClickHouseCluster(__file__)
node =  cluster.add_instance("node", main_configs=["configs/s3_cache.xml"], with_minio=True, stay_alive=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_bypass(started_cluster):
    node.query("DROP TABLE IF EXISTS t0")

    node.query(
        """
        CREATE TABLE t0 (key String, value String)
        ENGINE = MergeTree()
        PRIMARY KEY key
        SETTINGS storage_policy = 'basic_s3_policy'
        """
    )
    node.query(
        """
        SYSTEM ENABLE FAILPOINT cache_filesystem_failure;
        """
    )
    node.query(
        """
        INSERT INTO t0 VALUES ('key0', 'value0'), ('key1', 'value1'), ('key2', 'value2')
        """
    )
    node.query(
        """
        SELECT * FROM t0
        """
    )
    node.restart_clickhouse()
    node.query(
        """
        SELECT * FROM t0
        """
    )
    assert node.query("SELECT count(*) FROM system.detached_parts WHERE table = 't0'") == '0\n'
    node.query("DROP TABLE t0")
