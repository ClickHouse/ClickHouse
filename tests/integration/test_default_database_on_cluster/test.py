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
ch3 = cluster.add_instance(
    "ch3",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
)
ch4 = cluster.add_instance(
    "ch4",
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
        sql="CREATE TABLE test_local_table ON CLUSTER 'cluster' (column UInt8) ENGINE = Memory;",
    )

    for node in [ch1, ch2, ch3, ch4]:
        assert (
            node.query("SHOW TABLES FROM test_default_database FORMAT TSV")
            == "test_local_table\n"
        )

    ch1.query(
        database="test_default_database",
        sql="CREATE TABLE test_distributed_table ON CLUSTER 'cluster' (column UInt8) ENGINE = Distributed(cluster, currentDatabase(), 'test_local_table');",
    )

    for node in [ch1, ch2, ch3, ch4]:
        assert (
            node.query("SHOW TABLES FROM test_default_database FORMAT TSV")
            == "test_distributed_table\ntest_local_table\n"
        )
        assert (
            node.query(
                "SHOW CREATE TABLE test_default_database.test_distributed_table FORMAT TSV"
            )
            == "CREATE TABLE test_default_database.test_distributed_table\\n(\\n    `column` UInt8\\n)\\nENGINE = Distributed(\\'cluster\\', \\'test_default_database\\', \\'test_local_table\\')\n"
        )
