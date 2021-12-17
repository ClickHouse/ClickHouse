import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, exec_query_with_retry
cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=["configs/clusters.xml"])
node2 = cluster.add_instance('node2', main_configs=["configs/clusters.xml"])
node3 = cluster.add_instance('node3', main_configs=["configs/clusters.xml"])
node4 = cluster.add_instance('node4', main_configs=["configs/clusters.xml"], image='yandex/clickhouse-server', tag='21.5')

def insert_data(node, table_name):
    node.query("""INSERT INTO {}
                VALUES (bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));""".format(table_name))

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_groupBitmapAnd_on_distributed_table(start_cluster):
    local_table_name = "bitmap_column_expr_test"
    distributed_table_name = "bitmap_column_expr_test_dst"
    cluster_name = "awsome_cluster"

    node1.query("""CREATE TABLE {} on cluster {}
    (
        z AggregateFunction(groupBitmap, UInt32)
    )
    ENGINE = MergeTree()
    ORDER BY tuple()""".format(local_table_name, cluster_name))

    node1.query("""CREATE TABLE {0} on cluster {1}
    (
        z AggregateFunction(groupBitmap, UInt32)
    )
    ENGINE = Distributed({1}, currentDatabase(), '{0}')""".format(distributed_table_name, cluster_name))
    insert_data(node1, local_table_name)

    expected = "10"
    node1_result = node1.query("select groupBitmapAnd(z) FROM {};".format(distributed_table_name)).strip()
    node2_result = node1.query("select groupBitmapAnd(z) FROM {};".format(distributed_table_name)).strip()
    assert(node1_result == expected)
    assert(node2_result == expected)

def test_aggregate_function_versioning(start_cluster):
    local_table_name = "bitmap_column_expr_versioning_test"
    distributed_table_name = "bitmap_column_expr_versioning_test_dst"
    cluster_name = "test_version_cluster"

    node3.query("""CREATE TABLE {} on cluster {}
    (
        z AggregateFunction(groupBitmap, UInt32)
    )
    ENGINE = MergeTree()
    ORDER BY tuple()""".format(local_table_name, cluster_name))

    node3.query("""CREATE TABLE {0} on cluster {1}
    (
        z AggregateFunction(groupBitmap, UInt32)
    )
    ENGINE = Distributed({1}, currentDatabase(), '{0}')""".format(distributed_table_name, cluster_name))

    insert_data(node3, local_table_name)
    insert_data(node4, local_table_name)

    expected = "10"
    new_version_distributed_result = node3.query("select groupBitmapAnd(z) FROM {};".format(distributed_table_name)).strip()
    old_version_distributed_result = node4.query("select groupBitmapAnd(z) FROM {};".format(distributed_table_name)).strip()
    assert(new_version_distributed_result == expected)
    assert(old_version_distributed_result == expected)

    result_from_old_to_new_version = node3.query("select groupBitmapAnd(z) FROM  remote('node4', default.{})".format(local_table_name)).strip()
    assert(result_from_old_to_new_version != expected)

    result_from_new_to_old_version = node4.query("select groupBitmapAnd(z) FROM  remote('node3', default.{})".format(local_table_name)).strip()
    assert(result_from_new_to_old_version != expected)
