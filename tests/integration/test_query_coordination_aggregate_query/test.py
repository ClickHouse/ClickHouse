import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 1, "replica": 1},)
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 1, "replica": 2},)
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 2, "replica": 1},)
node4 = cluster.add_instance("node4", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 2, "replica": 2},)
node5 = cluster.add_instance("node5", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 3, "replica": 1},)
node6 = cluster.add_instance("node6", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 3, "replica": 2},)
# test_two_shards

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node1.query(
            """CREATE TABLE local_table ON CLUSTER test_two_shards (id UInt32, val String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/local_table', '{replica}') ORDER BY id SETTINGS index_granularity=100;"""
        )

        node1.query(
            """CREATE TABLE distributed_table ON CLUSTER test_two_shards (id UInt32, val String) ENGINE = Distributed(test_two_shards, default, local_table, rand());"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_aggregate_query(started_cluster):
    node1.query("INSERT INTO distributed_table SELECT * FROM generateRandom('id Int, val String') LIMIT 20")

    node1.query("INSERT INTO distributed_table SELECT id,'111' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'222' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'333' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'444' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'555' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'666' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'777' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'888' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'999' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'100' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'101' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'102' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'103' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'104' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'105' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'106' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'107' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'108' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'109' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'110' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'211' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'212' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'213' FROM generateRandom('id Int') LIMIT 203")

    node1.query("SYSTEM FLUSH DISTRIBUTED distributed_table")

    print("local table select:")
    r = node1.query("SELECT sum(id),val FROM local_table GROUP BY val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 0")
    print(r)

    print("distribute table select:")
    rr = node1.query("SELECT sum(id),val FROM distributed_table GROUP BY val")
    print(rr)

    print("local table select:")
    r = node1.query("SELECT uniq(id),val FROM local_table GROUP BY val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 0")
    print(r)

    print("distribute table select:")
    rr = node1.query("SELECT uniq(id),val FROM distributed_table GROUP BY val")
    print(rr)
