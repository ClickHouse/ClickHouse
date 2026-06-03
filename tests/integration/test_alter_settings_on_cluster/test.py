import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
)
ch2 = cluster.add_instance(
    "ch2",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        ch1.query("CREATE DATABASE test_default_database ON CLUSTER 'cluster';")
        yield cluster

    finally:
        cluster.shutdown()


def test_default_database_on_cluster(started_cluster):
    ch1.query(
        database="test_default_database",
        sql="CREATE TABLE test_local_table (x UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_local_table', 'r1') ORDER BY tuple();",
    )

    ch2.query(
        database="test_default_database",
        sql="CREATE TABLE test_local_table (x UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_local_table', 'r2') ORDER BY tuple();",
    )

    ch1.query(
        database="test_default_database",
        sql="ALTER TABLE test_local_table ON CLUSTER 'cluster' MODIFY SETTING old_parts_lifetime = 100;",
    )

    for node in [ch1, ch2]:
        assert node.query(
            database="test_default_database",
            sql="SHOW CREATE test_local_table FORMAT TSV",
        ).endswith("old_parts_lifetime = 100\n")

    ch1.query_and_get_error(
        database="test_default_database",
        sql="ALTER TABLE test_local_table MODIFY SETTING temporary_directories_lifetime = 1 RESET SETTING old_parts_lifetime;",
    )

    ch1.query_and_get_error(
        database="test_default_database",
        sql="ALTER TABLE test_local_table RESET SETTING old_parts_lifetime MODIFY SETTING temporary_directories_lifetime = 1;",
    )

    ch1.query(
        database="test_default_database",
        sql="ALTER TABLE test_local_table ON CLUSTER 'cluster' RESET SETTING old_parts_lifetime;",
    )

    for node in [ch1, ch2]:
        assert not node.query(
            database="test_default_database",
            sql="SHOW CREATE test_local_table FORMAT TSV",
        ).endswith("old_parts_lifetime = 100\n")

    ch1.query(
        database="test_default_database",
        sql="DROP TABLE test_local_table ON CLUSTER 'cluster' SYNC",
    )
