import pytest
from helpers.cluster import ClickHouseCluster, QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
    # table name is too long, exceeds S3 object name maximum
    with_remote_database_disk=False,
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
    # table name is too long, exceeds S3 object name maximum
    with_remote_database_disk=False,
)
nodes = [node1, node2]

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_check_table_name_length_2(started_cluster):
    for node in nodes:
        node.query("drop database if exists atomic")
        node.query("drop database if exists replic")

    for (i, node) in enumerate(nodes):
        node.query("create database atomic engine Atomic;"
                   f"create database replic engine Replicated('/replic' , 's0', 'r{i}');")

    # (For simplicity we assume that filesystem's file name limit is 255. If we ever need to run
    #  this test in environments where the limit is different, these numbers can be replaced with
    #  something based on getMaxTableNameLengthForDatabase.)
    ok_name = 'a'*200
    long_name = 'a'*250
    long_db = 'd'*100

    node1.query(f"create table atomic.{ok_name} (x Int64) engine MergeTree order by x")
    node1.query(f"create table replic.{ok_name} (x Int64) engine MergeTree order by x")
    node2.query(f"create or replace table atomic.{ok_name} on cluster 'two_replicas' (x Int64) engine MergeTree order by x")

    for (node, db, on_cluster) in [(node1, "atomic", ""), (node2, "replic", ""), (node1, "atomic", "on cluster 'two_replicas'")]:
        # CREATE TABLE
        with pytest.raises(QueryRuntimeException, match="ARGUMENT_OUT_OF_BOUND"):
            node.query(f"create table {db}.{long_name} {on_cluster} (x Int64) engine MergeTree order by x")
        # CREATE OR REPLACE TABLE
        with pytest.raises(QueryRuntimeException, match="ARGUMENT_OUT_OF_BOUND"):
            node.query(f"create or replace table {db}.{long_name} {on_cluster} (x Int64) engine MergeTree order by x")
        # RENAME TABLE
        with pytest.raises(QueryRuntimeException, match="ARGUMENT_OUT_OF_BOUND"):
            node.query(f"rename table {db}.{ok_name} to atomic.{long_name} {on_cluster}")
        # RENAME DATABASE
        with pytest.raises(QueryRuntimeException, match="ARGUMENT_OUT_OF_BOUND"):
            node.query(f"rename database {db} to f{long_db} {on_cluster}")

    for node in nodes:
        node.query(
            "drop database atomic;"
            "drop database replic;")
