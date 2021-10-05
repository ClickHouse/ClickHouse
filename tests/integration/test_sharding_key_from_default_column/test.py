import pytest
import itertools
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/test_cluster.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/test_cluster.xml'], with_zookeeper=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        node1.query("DROP TABLE IF EXISTS dist ON CLUSTER 'test_cluster'")
        node1.query("DROP TABLE IF EXISTS local ON CLUSTER 'test_cluster'")


# A default column is used in the sharding key expression.
def test_default_column():
    node1.query("CREATE TABLE dist ON CLUSTER 'test_cluster' (x Int32, y Int32 DEFAULT x + 100, z Int32 DEFAULT x + y) ENGINE = Distributed('test_cluster', currentDatabase(), local, y)")
    node1.query("CREATE TABLE local ON CLUSTER 'test_cluster' (x Int32, y Int32 DEFAULT x + 200, z Int32 DEFAULT x - y) ENGINE = MergeTree() ORDER BY y")

    for insert_sync in [0, 1]:
        settings = {'insert_distributed_sync': insert_sync}
        
        # INSERT INTO TABLE dist (x)
        node1.query("TRUNCATE TABLE local ON CLUSTER 'test_cluster'")
        node1.query("INSERT INTO TABLE dist (x) VALUES (1), (2), (3), (4)", settings=settings)
        node1.query("SYSTEM FLUSH DISTRIBUTED dist")
        assert node1.query("SELECT x, y, z FROM local") == TSV([[2, 102, 104], [4, 104, 108]])
        assert node2.query("SELECT x, y, z FROM local") == TSV([[1, 101, 102], [3, 103, 106]])
        assert node1.query("SELECT x, y, z FROM dist") == TSV([[2, 102, 104], [4, 104, 108], [1, 101, 102], [3, 103, 106]])

        # INSERT INTO TABLE dist (x, y)
        node1.query("TRUNCATE TABLE local ON CLUSTER 'test_cluster'")
        node1.query("INSERT INTO TABLE dist (x, y) VALUES (1, 11), (2, 22), (3, 33)", settings=settings)
        node1.query("SYSTEM FLUSH DISTRIBUTED dist")
        assert node1.query("SELECT x, y, z FROM local") == TSV([[2, 22, 24]])
        assert node2.query("SELECT x, y, z FROM local") == TSV([[1, 11, 12], [3, 33, 36]])
        assert node1.query("SELECT x, y, z FROM dist") == TSV([[2, 22, 24], [1, 11, 12], [3, 33, 36]])


# A materialized column is used in the sharding key expression and `insert_allow_materialized_columns` set to 1.
def test_materialized_column_allow_insert_materialized():
    node1.query("CREATE TABLE dist ON CLUSTER 'test_cluster' (x Int32, y Int32 MATERIALIZED x + 100, z Int32 MATERIALIZED x + y) ENGINE = Distributed('test_cluster', currentDatabase(), local, y)")
    node1.query("CREATE TABLE local ON CLUSTER 'test_cluster' (x Int32, y Int32 MATERIALIZED x + 200, z Int32 MATERIALIZED x - y) ENGINE = MergeTree() ORDER BY y")

    for insert_sync in [0, 1]:
        settings = {'insert_distributed_sync': insert_sync, 'insert_allow_materialized_columns': 1}
        
        # INSERT INTO TABLE dist (x)
        node1.query("TRUNCATE TABLE local ON CLUSTER 'test_cluster'")
        node1.query("INSERT INTO TABLE dist (x) VALUES (1), (2), (3), (4)", settings=settings)
        node1.query("SYSTEM FLUSH DISTRIBUTED dist")
        assert node1.query("SELECT x, y, z FROM local") == TSV([[2, 102, 104], [4, 104, 108]])
        assert node2.query("SELECT x, y, z FROM local") == TSV([[1, 101, 102], [3, 103, 106]])
        assert node1.query("SELECT x, y, z FROM dist") == TSV([[2, 102, 104], [4, 104, 108], [1, 101, 102], [3, 103, 106]])

        # INSERT INTO TABLE dist (x, y)
        node1.query("TRUNCATE TABLE local ON CLUSTER 'test_cluster'")
        node1.query("INSERT INTO TABLE dist (x, y) VALUES (1, 11), (2, 22), (3, 33)", settings=settings)
        node1.query("SYSTEM FLUSH DISTRIBUTED dist")
        assert node1.query("SELECT x, y, z FROM local") == TSV([[2, 22, 24]])
        assert node2.query("SELECT x, y, z FROM local") == TSV([[1, 11, 12], [3, 33, 36]])
        assert node1.query("SELECT x, y, z FROM dist") == TSV([[2, 22, 24], [1, 11, 12], [3, 33, 36]])


# A materialized column is used in the sharding key expression and `insert_allow_materialized_columns` set to 0.
def test_materialized_column_disallow_insert_materialized():
    node1.query("CREATE TABLE dist ON CLUSTER 'test_cluster' (x Int32, y Int32 MATERIALIZED x + 100, z Int32 MATERIALIZED x + y) ENGINE = Distributed('test_cluster', currentDatabase(), local, y)")
    node1.query("CREATE TABLE local ON CLUSTER 'test_cluster' (x Int32, y Int32 MATERIALIZED x + 200, z Int32 MATERIALIZED x - y) ENGINE = MergeTree() ORDER BY y")

    for insert_sync in [0, 1]:
        settings = {'insert_distributed_sync': insert_sync, 'insert_allow_materialized_columns': 0}
        
        # INSERT INTO TABLE dist (x)
        node1.query("TRUNCATE TABLE local ON CLUSTER 'test_cluster'")
        node1.query("INSERT INTO TABLE dist (x) VALUES (1), (2), (3), (4)", settings=settings)
        node1.query("SYSTEM FLUSH DISTRIBUTED dist")
        assert node1.query("SELECT x, y, z FROM local") == TSV([[2, 202, -200], [4, 204, -200]])
        assert node2.query("SELECT x, y, z FROM local") == TSV([[1, 201, -200], [3, 203, -200]])
        assert node1.query("SELECT x, y, z FROM dist") == TSV([[2, 202, -200], [4, 204, -200], [1, 201, -200], [3, 203, -200]])

        # INSERT INTO TABLE dist (x, y)
        node1.query("TRUNCATE TABLE local ON CLUSTER 'test_cluster'")
        expected_error = "Cannot insert column y, because it is MATERIALIZED column"
        assert expected_error in node1.query_and_get_error("INSERT INTO TABLE dist (x, y) VALUES (1, 11), (2, 22), (3, 33)", settings=settings)


# Almost the same as the previous test `test_materialized_column_disallow_insert_materialized`, but the sharding key has different values.
def test_materialized_column_disallow_insert_materialized_different_shards():
    node1.query("CREATE TABLE dist ON CLUSTER 'test_cluster' (x Int32, y Int32 MATERIALIZED x + 101, z Int32 MATERIALIZED x + y) ENGINE = Distributed('test_cluster', currentDatabase(), local, y)")
    node1.query("CREATE TABLE local ON CLUSTER 'test_cluster' (x Int32, y Int32 MATERIALIZED x + 200, z Int32 MATERIALIZED x - y) ENGINE = MergeTree() ORDER BY y")

    for insert_sync in [0, 1]:
        settings = {'insert_distributed_sync': insert_sync, 'insert_allow_materialized_columns': 0}
        
        # INSERT INTO TABLE dist (x)
        node1.query("TRUNCATE TABLE local ON CLUSTER 'test_cluster'")
        node1.query("INSERT INTO TABLE dist (x) VALUES (1), (2), (3), (4)", settings=settings)
        node1.query("SYSTEM FLUSH DISTRIBUTED dist")
        assert node1.query("SELECT x, y, z FROM local") == TSV([[1, 201, -200], [3, 203, -200]])
        assert node2.query("SELECT x, y, z FROM local") == TSV([[2, 202, -200], [4, 204, -200]])
        assert node1.query("SELECT x, y, z FROM dist") == TSV([[1, 201, -200], [3, 203, -200], [2, 202, -200], [4, 204, -200]])
