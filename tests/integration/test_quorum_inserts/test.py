import os
import sys
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        clusters_schema = {
            "0" : {"0" : ["0", "1", "2"]}
        }

        for cluster_name, shards in clusters_schema.iteritems():
            for shard_name, replicas in shards.iteritems():
                for replica_name in replicas:
                    name = "s{}_{}_{}".format(cluster_name, shard_name, replica_name)
                    cluster.add_instance(name,
                                         config_dir="configs",
                                         macros={"cluster": cluster_name, "shard": shard_name, "replica": replica_name},
                                         with_zookeeper=True)

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

def test_drop_replica_and_achieve_quorum(started_cluster):
    zero = cluster.instances['s0_0_0']
    first = cluster.instances['s0_0_1']
    second = cluster.instances['s0_0_2']

    zero.query("DROP DATABASE IF EXISTS bug ON CLUSTER one_shard_three_replicas")
    zero.query("CREATE DATABASE IF NOT EXISTS bug ON CLUSTER one_shard_three_replicas")

    create_query = "CREATE TABLE bug.test_drop_replica_and_achieve_quorum " \
                   "(a Int8, d Date) " \
                   "Engine = ReplicatedMergeTree('/clickhouse/tables/test_drop_replica_and_achieve_quorum', '{}') " \
                   "PARTITION BY d ORDER BY a"

    print("Create Replicated table with two replicas")
    zero.query(create_query.format(0))
    first.query(create_query.format(1))

    print("Stop fetches on one replica. Since that, it will be isolated.")
    first.query("SYSTEM STOP FETCHES bug.test_drop_replica_and_achieve_quorum")

    print("Insert to other replica. This query will fail.")
    quorum_timeout = zero.query_and_get_error("INSERT INTO bug.test_drop_replica_and_achieve_quorum(a,d) VALUES (1, '2011-01-01')")
    assert "Timeout while waiting for quorum" in quorum_timeout, "Query must fail."

    assert "1\t2011-01-01\n" == zero.query("SELECT * FROM bug.test_drop_replica_and_achieve_quorum",
                                          settings={'select_sequential_consistency' : 0})

    print("Add third replica")
    second.query(create_query.format(2))

    zero.query("SYSTEM RESTART REPLICA bug.test_drop_replica_and_achieve_quorum")

    print("START FETCHES first replica")
    first.query("SYSTEM START FETCHES bug.test_drop_replica_and_achieve_quorum")

    time.sleep(5)

    print(zero.query("SELECT * from system.replicas format Vertical"))


    print("---------")
    print(zero.query("SELECT * from system.replication_queue format Vertical"))
    print("---------")


    print(first.query("SELECT * from system.replicas format Vertical"))
    print("---------")
    print(first.query("SELECT * from system.replication_queue format Vertical"))
    print("---------")
    print(second.query("SELECT * from system.replicas format Vertical"))
    print("---------")
    print(first.query("SELECT * from system.replication_queue format Vertical"))


    print("SYNC first replica")
    first.query("SYSTEM SYNC REPLICA bug.test_drop_replica_and_achieve_quorum")

    print("SYNC second replica")
    second.query("SYSTEM SYNC REPLICA bug.test_drop_replica_and_achieve_quorum")

    print("Quorum for previous insert achieved.")
    assert "1\t2011-01-01\n" == second.query("SELECT * FROM bug.test_drop_replica_and_achieve_quorum",
                                            settings={'select_sequential_consistency' : 1})

    print("Now we can insert some other data.")
    zero.query("INSERT INTO bug.test_drop_replica_and_achieve_quorum(a,d) VALUES (2, '2012-02-02')")

    assert "1\t2011-01-01\n2 2012-02-02" == zero.query("SELECT * FROM bug.test_drop_replica_and_achieve_quorum")
    assert "1\t2011-01-01\n2 2012-02-02" == second.query("SELECT * FROM bug.test_drop_replica_and_achieve_quorum")

    zero.query("DROP DATABASE IF EXISTS bug ON CLUSTER one_shard_three_replicas")


def test_insert_quorum_with_drop_partition(started_cluster):
    zero = cluster.instances['s0_0_0']
    first = cluster.instances['s0_0_1']
    second = cluster.instances['s0_0_2']

    zero.query("DROP DATABASE IF EXISTS bug ON CLUSTER one_shard_three_replicas")
    zero.query("CREATE DATABASE IF NOT EXISTS bug ON CLUSTER one_shard_three_replicas")

    zero.query("CREATE TABLE bug.quorum_insert_with_drop_partition ON CLUSTER one_shard_three_replicas "
               "(a Int8, d Date) "
               "Engine = ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}') "
               "PARTITION BY d ORDER BY a ")

    print("Stop fetches for bug.quorum_insert_with_drop_partition at first replica.")
    first.query("SYSTEM STOP FETCHES bug.quorum_insert_with_drop_partition")

    print("Insert with quorum. (zero and second)")
    zero.query_and_get_error("INSERT INTO bug.quorum_insert_with_drop_partition(a,d) VALUES(1, '2011-01-01')")

    print("Drop partition.")
    zero.query_and_get_error("ALTER TABLE bug.quorum_insert_with_drop_partition DROP PARTITION '2011-01-01'")

    print("Insert to deleted partition")
    zero.query_and_get_error("INSERT INTO bug.quorum_insert_with_drop_partition(a,d) VALUES(2, '2011-01-01')")

    print("Sync other replica from quorum.")
    second.query("SYSTEM SYNC REPLICA bug.quorum_insert_with_drop_partition")

    print("Select from updated partition.")
    assert "2 2011-01-01\n" == zero.query("SELECT * FROM bug.quorum_insert_with_drop_partition")
    assert "2 2011-01-01\n" == second.query("SELECT * FROM bug.quorum_insert_with_drop_partition")

    zero.query("DROP DATABASE IF EXISTS bug ON CLUSTER one_shard_three_replicas")


def test_insert_quorum_with_ttl(started_cluster):
    zero = cluster.instances['s0_0_0']
    first = cluster.instances['s0_0_1']

    zero.query("DROP DATABASE IF EXISTS bug ON CLUSTER one_shard_two_replicas")
    zero.query("CREATE DATABASE IF NOT EXISTS bug ON CLUSTER one_shard_two_replicas")

    zero.query("CREATE TABLE bug.quorum_insert_with_ttl ON CLUSTER one_shard_two_replicas "
               "(a Int8, d Date) "
               "Engine = ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}') "
               "PARTITION BY d ORDER BY a "
               "TTL d + INTERVAL 5 second "
               "SETTINGS merge_with_ttl_timeout=2 ")

    print("Stop fetches for bug.quorum_insert_with_ttl at first replica.")
    first.query("SYSTEM STOP FETCHES bug.quorum_insert_with_ttl")

    print("Insert should fail since it can not reach the quorum.")
    quorum_timeout = zero.query_and_get_error("INSERT INTO bug.quorum_insert_with_ttl(a,d) VALUES(6, now())")
    assert "Timeout while waiting for quorum" in quorum_timeout, "Query must fail."

    print("Wait 10 seconds and the data should be dropped by TTL.")
    time.sleep(10)
    count = zero.query("SELECT count() FROM bug.quorum_insert_with_ttl WHERE a=6")
    assert count == "0\n", "Data have to be dropped by TTL"

    print("Resume fetches for bug.quorum_test_with_ttl at first replica.")
    first.query("SYSTEM STOP FETCHES bug.quorum_insert_with_ttl")
    time.sleep(5)

    print("Inserts should resume.")
    zero.query("INSERT INTO bug.quorum_insert_with_ttl(a) VALUES(6)")
