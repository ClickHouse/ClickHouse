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
        yield cluster

    finally:
        cluster.shutdown()



def test_in_and_join(started_cluster):
    ch1.query("CREATE DATABASE test_default_database ON CLUSTER 'cluster';")

    ch1.query(
        database="test_default_database",
        sql="CREATE TABLE t1 (A Int64) ENGINE = MergeTree ORDER BY A AS SELECT * FROM values((1),(2),(3));",
    )

    ch2.query(
        database="test_default_database",
        sql="""
CREATE TABLE t1 (A Int64) ENGINE = MergeTree ORDER BY A;
CREATE TABLE t1_d AS t1 ENGINE = Distributed('cluster', currentDatabase(), t1);
CREATE TABLE t2 (A Int64) ENGINE = MergeTree ORDER BY A AS SELECT * FROM values((1),(2));
        """
    )

    assert (
        ch2.query(
            database="test_default_database",
            sql="SELECT * FROM t1_d WHERE A IN ( SELECT A FROM t2 ) SETTINGS prefer_global_in_and_join = 1, enable_analyzer = 1"
        ) == "1\n2\n"
    )

    assert (
        ch2.query(
            database="test_default_database",
            sql="SELECT * FROM t1_d JOIN t2 USING (A) JOIN t2 USING (A) SETTINGS prefer_global_in_and_join = 1, enable_analyzer = 1"
        ) == "1\n2\n"
    )

    assert (
        ch2.query(
            database="test_default_database",
            sql="SELECT * FROM t1_d JOIN t2 USING (A) SETTINGS prefer_global_in_and_join = 1, enable_analyzer = 1"
        ) == "1\n2\n"
    )

    ch1.query("DROP DATABASE IF EXISTS test_default_database ON CLUSTER 'cluster' SYNC")
