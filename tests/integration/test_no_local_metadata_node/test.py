import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_table_start_without_metadata(start_cluster):
    node1.query("""
        CREATE TABLE test (date Date)
        ENGINE = ReplicatedMergeTree('/clickhouse/table/test_table', '1')
        ORDER BY tuple()
    """)

    node1.query("INSERT INTO test VALUES(toDate('2019-12-01'))")

    assert node1.query("SELECT date FROM test") == "2019-12-01\n"

    # some fake alter
    node1.query("ALTER TABLE test MODIFY COLUMN date Date DEFAULT toDate('2019-10-01')")

    assert node1.query("SELECT date FROM test") == "2019-12-01\n"

    node1.query("DETACH TABLE test")
    zk_cli = cluster.get_kazoo_client('zoo1')

    # simulate update from old version
    zk_cli.delete("/clickhouse/table/test_table/replicas/1/metadata")
    zk_cli.delete("/clickhouse/table/test_table/replicas/1/metadata_version")

    node1.query("ATTACH TABLE test")

    assert node1.query("SELECT date FROM test") == "2019-12-01\n"

    node1.query("ALTER TABLE test MODIFY COLUMN date Date DEFAULT toDate('2019-09-01')")

    node1.query("DETACH TABLE test")

    zk_cli.set("/clickhouse/table/test_table/replicas/1/metadata", b"")

    node1.query("ATTACH TABLE test")

    assert node1.query("SELECT date FROM test") == "2019-12-01\n"
