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
            """CREATE TABLE local_table ON CLUSTER test_two_shards (id UInt32, val String, name String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/local_table', '{replica}') ORDER BY id SETTINGS index_granularity=100;"""
        )

        node1.query(
            """CREATE TABLE distributed_table ON CLUSTER test_two_shards (id UInt32, val String, name String) ENGINE = Distributed(test_two_shards, default, local_table, rand());"""
        )

        node1.query(
            """CREATE TABLE local_table1 ON CLUSTER test_two_shards (id UInt32, val String, str String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/local_table1', '{replica}') ORDER BY id SETTINGS index_granularity=100;"""
        )

        node1.query(
            """CREATE TABLE distributed_table1 ON CLUSTER test_two_shards (id UInt32, val String, str String) ENGINE = Distributed(test_two_shards, default, local_table1, rand());"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_aggregate_query(started_cluster):
    node1.query("INSERT INTO distributed_table SELECT id,'113','bsada' FROM generateRandom('id Int') LIMIT 233")

    node1.query("INSERT INTO distributed_table SELECT id,'111','dsada' FROM generateRandom('id Int') LIMIT 213")
    node1.query("INSERT INTO distributed_table SELECT id,'222','dqada' FROM generateRandom('id Int') LIMIT 253")
    node1.query("INSERT INTO distributed_table SELECT id,'333','dwada' FROM generateRandom('id Int') LIMIT 309")
    node1.query("INSERT INTO distributed_table SELECT id,'444','deada' FROM generateRandom('id Int') LIMIT 204")
    node1.query("INSERT INTO distributed_table SELECT id,'555','drada' FROM generateRandom('id Int') LIMIT 273")
    node1.query("INSERT INTO distributed_table SELECT id,'666','dtada' FROM generateRandom('id Int') LIMIT 293")
    node1.query("INSERT INTO distributed_table SELECT id,'777','dyada' FROM generateRandom('id Int') LIMIT 223")
    node1.query("INSERT INTO distributed_table SELECT id,'888','duada' FROM generateRandom('id Int') LIMIT 233")
    node1.query("INSERT INTO distributed_table SELECT id,'999','diada' FROM generateRandom('id Int') LIMIT 223")
    node1.query("INSERT INTO distributed_table SELECT id,'100','doada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'101','dpada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'102','dsada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'103','dsada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'104','dfada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'105','dgada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'106','dhada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'107','djada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'108','dkada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table SELECT id,'109','dlada' FROM generateRandom('id Int') LIMIT 100")
    node1.query("INSERT INTO distributed_table SELECT id,'110','dpada' FROM generateRandom('id Int') LIMIT 90")
    node1.query("INSERT INTO distributed_table SELECT id,'211','dxada' FROM generateRandom('id Int') LIMIT 22")
    node1.query("INSERT INTO distributed_table SELECT id,'212','dcada' FROM generateRandom('id Int') LIMIT 103")
    node1.query("INSERT INTO distributed_table SELECT id,'213','dvada' FROM generateRandom('id Int') LIMIT 21")


    node1.query("INSERT INTO distributed_table1 SELECT id,'913','bsada' FROM generateRandom('id Int') LIMIT 233")

    node1.query("INSERT INTO distributed_table1 SELECT id,'911','doada' FROM generateRandom('id Int') LIMIT 213")
    node1.query("INSERT INTO distributed_table1 SELECT id,'322','dpada' FROM generateRandom('id Int') LIMIT 253")
    node1.query("INSERT INTO distributed_table1 SELECT id,'433','dwada' FROM generateRandom('id Int') LIMIT 309")
    node1.query("INSERT INTO distributed_table1 SELECT id,'544','deada' FROM generateRandom('id Int') LIMIT 204")
    node1.query("INSERT INTO distributed_table1 SELECT id,'655','dhada' FROM generateRandom('id Int') LIMIT 273")
    node1.query("INSERT INTO distributed_table1 SELECT id,'766','dcada' FROM generateRandom('id Int') LIMIT 293")
    node1.query("INSERT INTO distributed_table1 SELECT id,'877','dyada' FROM generateRandom('id Int') LIMIT 223")
    node1.query("INSERT INTO distributed_table1 SELECT id,'888','dmada' FROM generateRandom('id Int') LIMIT 233")
    node1.query("INSERT INTO distributed_table1 SELECT id,'999','dvada' FROM generateRandom('id Int') LIMIT 223")
    node1.query("INSERT INTO distributed_table1 SELECT id,'900','doada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table1 SELECT id,'901','dcada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table1 SELECT id,'902','dsada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table1 SELECT id,'903','dsada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table1 SELECT id,'904','dlada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table1 SELECT id,'905','dgada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table1 SELECT id,'106','dhtda' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table1 SELECT id,'107','djada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table1 SELECT id,'108','dkada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO distributed_table1 SELECT id,'109','dlada' FROM generateRandom('id Int') LIMIT 100")
    node1.query("INSERT INTO distributed_table1 SELECT id,'110','dpada' FROM generateRandom('id Int') LIMIT 90")
    node1.query("INSERT INTO distributed_table1 SELECT id,'211','dzada' FROM generateRandom('id Int') LIMIT 22")
    node1.query("INSERT INTO distributed_table1 SELECT id,'212','dcada' FROM generateRandom('id Int') LIMIT 103")
    node1.query("INSERT INTO distributed_table1 SELECT id,'213','dvada' FROM generateRandom('id Int') LIMIT 21")

    node1.query("SYSTEM FLUSH DISTRIBUTED distributed_table")
    node1.query("SYSTEM FLUSH DISTRIBUTED distributed_table1")

    r = node1.query("EXPLAIN SELECT sum(id) ids,val FROM distributed_table where name= 'djada' GROUP BY val ORDER BY ids")
    print(r)

    r = node1.query("EXPLAIN SELECT sum(id) ids,val FROM local_table where name= 'djada' GROUP BY val ORDER BY ids")
    print(r)

    print("local table select:")
    r = node1.query("SELECT sum(id),val FROM local_table GROUP BY val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 0")
    print(r)

    print("distribute table select:")
    rr = node1.query("SELECT sum(id),val FROM distributed_table GROUP BY val")
    print(rr)

    print("local table select:")
    r = node1.query("SELECT sum(id),name,val FROM local_table GROUP BY name,val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 0")
    print(r)

    print("distribute table select:")
    rr = node1.query("SELECT sum(id),name,val FROM distributed_table GROUP BY name,val")
    print(rr)


    print("local table select:")
    r = node1.query("SELECT sum(id),name,val FROM local_table GROUP BY name,val ORDER BY val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 0")
    print(r)

    print("distribute table select:")
    rr = node1.query("SELECT sum(id),name,val FROM distributed_table GROUP BY name,val ORDER BY val")
    print(rr)

    assert r == rr

    print("local table select:")
    r = node1.query("SELECT sum(id),name,val FROM local_table GROUP BY name,val ORDER BY name,val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 0")
    print(r)

    print("distribute table select:")
    rr = node1.query("SELECT sum(id),name,val FROM distributed_table GROUP BY name,val ORDER BY name,val")
    print(rr)

    assert r == rr

    print("local table select:")
    r = node1.query("SELECT sum(id),name,val FROM local_table GROUP BY name,val ORDER BY name,val LIMIT 10 SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 0")
    print(r)

    print("distribute table select:")
    rr = node1.query("SELECT sum(id),name,val FROM distributed_table GROUP BY name,val ORDER BY name,val LIMIT 10 ")
    print(rr)

    assert r == rr

    print("local table select:")
    r = node1.query("SELECT uniq(id),val FROM local_table GROUP BY val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 0")
    print(r)

    print("distribute table select:")
    rr = node1.query("SELECT uniq(id),val FROM distributed_table GROUP BY val")
    print(rr)


    print("local table select:")
    r = node1.query("SELECT * FROM local_table a join local_table1 b on a.id=b.id order by a.id,a.val,a.name,b.val,b.str limit 30 SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 0")
    print(r)

    print("distribute table select:")
    rr = node1.query("SELECT * FROM distributed_table a GLOBAL join distributed_table1 b on a.id=b.id order by a.id,a.val,a.name,b.val,b.str limit 30")
    print(rr)

    assert r == rr
