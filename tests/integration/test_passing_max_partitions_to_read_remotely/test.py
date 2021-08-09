import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance("ch1")
ch2 = cluster.add_instance("ch2")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        ch1.query("CREATE DATABASE test_default_database;")
        yield cluster

    finally:
        cluster.shutdown()


def test_default_database_on_cluster(started_cluster):
    ch1.query(
        database="test_default_database",
        sql="CREATE TABLE test_local_table ENGINE MergeTree PARTITION BY i ORDER BY tuple() SETTINGS max_partitions_to_read = 1 AS SELECT arrayJoin([1, 2]) i;",
    )

    assert ch2.query(
        sql="SELECT * FROM remote('ch1:9000', test_default_database, test_local_table) ORDER BY i FORMAT TSV SETTINGS max_partitions_to_read = 0;",
    ) == "1\n2\n"
