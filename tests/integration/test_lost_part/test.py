#!/usr/bin/env python3

import pytest
import time
import ast
import random

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', with_zookeeper=True)
node2 = cluster.add_instance('node2', with_zookeeper=True)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def remove_part_from_disk(node, table, part_name):
    part_path = node.query(
        "SELECT path FROM system.parts WHERE table = '{}' and name = '{}'".format(table, part_name)).strip()
    if not part_path:
        raise Exception("Part " + part_name + "doesn't exist")
    node.exec_in_container(['bash', '-c', 'rm -r {p}/*'.format(p=part_path)], privileged=True)


def test_lost_part_same_replica(start_cluster):
    for node in [node1, node2]:
        node.query(
            "CREATE TABLE mt0 (id UInt64, date Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/t', '{}') ORDER BY tuple() PARTITION BY date".format(node.name))

    node1.query("SYSTEM STOP MERGES mt0")
    node2.query("SYSTEM STOP REPLICATION QUEUES")

    for i in range(5):
        node1.query("INSERT INTO mt0 VALUES ({}, toDate('2020-10-01'))".format(i))

    for i in range(20):
        parts_to_merge = node1.query("SELECT parts_to_merge FROM system.replication_queue")
        if parts_to_merge:
            parts_list = list(sorted(ast.literal_eval(parts_to_merge)))
            print("Got parts list", parts_list)
            if len(parts_list) < 3:
                raise Exception("Got too small parts list {}".format(parts_list))
            break
        time.sleep(1)

    victim_part_from_the_middle = random.choice(parts_list[1:-1])
    print("Will corrupt part", victim_part_from_the_middle)

    remove_part_from_disk(node1, 'mt0', victim_part_from_the_middle)

    node1.query("DETACH TABLE mt0")

    node1.query("ATTACH TABLE mt0")

    node1.query("SYSTEM START MERGES mt0")

    for i in range(10):
        result = node1.query("SELECT count() FROM system.replication_queue")
        if int(result) == 0:
            break
        time.sleep(1)
    else:
        assert False, "Still have something in replication queue:\n" + node1.query("SELECT count() FROM system.replication_queue FORMAT Vertical")

    assert node1.contains_in_log("Created empty part"), "Seems like empty part {} is not created or log message changed".format(victim_part_from_the_middle)

    assert node1.query("SELECT COUNT() FROM mt0") == "4\n"

    node2.query("SYSTEM START REPLICATION QUEUES")

    assert_eq_with_retry(node2, "SELECT COUNT() FROM mt0", "4")
    assert_eq_with_retry(node2, "SELECT COUNT() FROM system.replication_queue", "0")

def test_lost_part_other_replica(start_cluster):
    for node in [node1, node2]:
        node.query(
            "CREATE TABLE mt1 (id UInt64) ENGINE ReplicatedMergeTree('/clickhouse/tables/t1', '{}') ORDER BY tuple()".format(node.name))

    node1.query("SYSTEM STOP MERGES mt1")
    node2.query("SYSTEM STOP REPLICATION QUEUES")

    for i in range(5):
        node1.query("INSERT INTO mt1 VALUES ({})".format(i))

    for i in range(20):
        parts_to_merge = node1.query("SELECT parts_to_merge FROM system.replication_queue")
        if parts_to_merge:
            parts_list = list(sorted(ast.literal_eval(parts_to_merge)))
            print("Got parts list", parts_list)
            if len(parts_list) < 3:
                raise Exception("Got too small parts list {}".format(parts_list))
            break
        time.sleep(1)

    victim_part_from_the_middle = random.choice(parts_list[1:-1])
    print("Will corrupt part", victim_part_from_the_middle)

    remove_part_from_disk(node1, 'mt1', victim_part_from_the_middle)

    # other way to detect broken parts
    node1.query("CHECK TABLE mt1")

    node2.query("SYSTEM START REPLICATION QUEUES")

    for i in range(10):
        result = node2.query("SELECT count() FROM system.replication_queue")
        if int(result) == 0:
            break
        time.sleep(1)
    else:
        assert False, "Still have something in replication queue:\n" + node2.query("SELECT * FROM system.replication_queue FORMAT Vertical")

    assert node1.contains_in_log("Created empty part"), "Seems like empty part {} is not created or log message changed".format(victim_part_from_the_middle)

    assert_eq_with_retry(node2, "SELECT COUNT() FROM mt1", "4")
    assert_eq_with_retry(node2, "SELECT COUNT() FROM system.replication_queue", "0")

    node1.query("SYSTEM START MERGES mt1")

    assert_eq_with_retry(node1, "SELECT COUNT() FROM mt1", "4")
    assert_eq_with_retry(node1, "SELECT COUNT() FROM system.replication_queue", "0")

def test_lost_part_mutation(start_cluster):
    for node in [node1, node2]:
        node.query(
            "CREATE TABLE mt2 (id UInt64) ENGINE ReplicatedMergeTree('/clickhouse/tables/t2', '{}') ORDER BY tuple()".format(node.name))

    node1.query("SYSTEM STOP MERGES mt2")
    node2.query("SYSTEM STOP REPLICATION QUEUES")

    for i in range(2):
        node1.query("INSERT INTO mt2 VALUES ({})".format(i))

    node1.query("ALTER TABLE mt2 UPDATE id = 777 WHERE 1", settings={"mutations_sync": "0"})

    for i in range(20):
        parts_to_mutate = node1.query("SELECT count() FROM system.replication_queue")
        # two mutations for both replicas
        if int(parts_to_mutate) == 4:
            break
        time.sleep(1)

    remove_part_from_disk(node1, 'mt2', 'all_1_1_0')

    # other way to detect broken parts
    node1.query("CHECK TABLE mt2")

    node1.query("SYSTEM START MERGES mt2")

    for i in range(10):
        result = node1.query("SELECT count() FROM system.replication_queue")
        if int(result) == 0:
            break
        time.sleep(1)
    else:
        assert False, "Still have something in replication queue:\n" + node1.query("SELECT * FROM system.replication_queue FORMAT Vertical")

    assert_eq_with_retry(node1, "SELECT COUNT() FROM mt2", "1")
    assert_eq_with_retry(node1, "SELECT SUM(id) FROM mt2", "777")
    assert_eq_with_retry(node1, "SELECT COUNT() FROM system.replication_queue", "0")

    node2.query("SYSTEM START REPLICATION QUEUES")

    assert_eq_with_retry(node2, "SELECT COUNT() FROM mt2", "1")
    assert_eq_with_retry(node2, "SELECT SUM(id) FROM mt2", "777")
    assert_eq_with_retry(node2, "SELECT COUNT() FROM system.replication_queue", "0")


def test_lost_last_part(start_cluster):
    for node in [node1, node2]:
        node.query(
            "CREATE TABLE mt3 (id UInt64, p String) ENGINE ReplicatedMergeTree('/clickhouse/tables/t3', '{}') "
            "ORDER BY tuple() PARTITION BY p".format(node.name))

    node1.query("SYSTEM STOP MERGES mt3")
    node2.query("SYSTEM STOP REPLICATION QUEUES")

    for i in range(1):
        node1.query("INSERT INTO mt3 VALUES ({}, 'x')".format(i))

    # actually not important
    node1.query("ALTER TABLE mt3 UPDATE id = 777 WHERE 1", settings={"mutations_sync": "0"})

    partition_id = node1.query("select partitionId('x')").strip()
    remove_part_from_disk(node1, 'mt3', '{}_0_0_0'.format(partition_id))

    # other way to detect broken parts
    node1.query("CHECK TABLE mt3")

    node1.query("SYSTEM START MERGES mt3")

    for i in range(10):
        result = node1.query("SELECT count() FROM system.replication_queue")
        assert int(result) <= 1, "Have a lot of entries in queue {}".format(node1.query("SELECT * FROM system.replication_queue FORMAT Vertical"))
        if node1.contains_in_log("Cannot create empty part") and node1.contains_in_log("DROP/DETACH PARTITION"):
            break
        time.sleep(1)
    else:
        assert False, "Don't have required messages in node1 log"

    node1.query("ALTER TABLE mt3 DROP PARTITION ID '{}'".format(partition_id))

    assert_eq_with_retry(node1, "SELECT COUNT() FROM mt3", "0")
    assert_eq_with_retry(node1, "SELECT COUNT() FROM system.replication_queue", "0")
