#!/usr/bin/env python3

import ast
import random
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def remove_part_from_disk(node, table, part_name):
    part_path = node.query(
        f"SELECT path FROM system.parts WHERE table = '{table}' and name = '{part_name}'"
    ).strip()
    if not part_path:
        raise Exception("Part " + part_name + "doesn't exist")
    node.exec_in_container(
        ["bash", "-c", "rm -r {p}/*".format(p=part_path)], privileged=True
    )


def test_lost_part_same_replica(start_cluster):
    node1.query("DROP TABLE IF EXISTS mt0 SYNC")
    node2.query("DROP TABLE IF EXISTS mt0 SYNC")

    try:
        for node in [node1, node2]:
            node.query(
                f"CREATE TABLE mt0 (id UInt64, date Date) ENGINE ReplicatedMergeTree('/clickhouse/tables/t', '{node.name}') ORDER BY tuple() PARTITION BY date "
                "SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1, cleanup_thread_preferred_points_per_iteration=0,"
                "merge_selecting_sleep_ms=100, max_merge_selecting_sleep_ms=1000"
            )

        node1.query("SYSTEM STOP MERGES mt0")
        node2.query("SYSTEM STOP REPLICATION QUEUES")

        for i in range(5):
            node1.query(f"INSERT INTO mt0 VALUES ({i}, toDate('2020-10-01'))")

        for i in range(20):
            parts_to_merge = node1.query(
                "SELECT parts_to_merge FROM system.replication_queue WHERE table='mt0' AND length(parts_to_merge) > 0"
            )
            if parts_to_merge:
                parts_list = list(sorted(ast.literal_eval(parts_to_merge)))
                print("Got parts list", parts_list)
                if len(parts_list) < 3:
                    raise Exception(f"Got too small parts list {parts_list}")
                break
            time.sleep(1)

        victim_part_from_the_middle = random.choice(parts_list[1:-1])
        print("Will corrupt part", victim_part_from_the_middle)

        remove_part_from_disk(node1, "mt0", victim_part_from_the_middle)

        node1.query("DETACH TABLE mt0")

        node1.query("ATTACH TABLE mt0")

        node1.query("SYSTEM START MERGES mt0")
        res, err = node1.query_and_get_answer_with_error("SYSTEM SYNC REPLICA mt0")
        print("result: ", res)
        print("error: ", res)

        for i in range(10):
            result = node1.query("SELECT count() FROM system.replication_queue")
            if int(result) == 0:
                break
            time.sleep(1)
        else:
            assert False, "Still have something in replication queue:\n" + node1.query(
                "SELECT count() FROM system.replication_queue FORMAT Vertical"
            )

        assert node1.contains_in_log(
            f"Created empty part {victim_part_from_the_middle}"
        ), f"Seems like empty part {victim_part_from_the_middle} is not created or log message changed"

        assert node1.query("SELECT COUNT() FROM mt0") == "4\n"

        node2.query("SYSTEM START REPLICATION QUEUES")

        assert_eq_with_retry(node2, "SELECT COUNT() FROM mt0", "4")
        assert_eq_with_retry(node2, "SELECT COUNT() FROM system.replication_queue", "0")
    finally:
        node1.query("DROP TABLE IF EXISTS mt0 SYNC")
        node2.query("DROP TABLE IF EXISTS mt0 SYNC")


def test_lost_part_other_replica(start_cluster):
    node1.query("DROP TABLE IF EXISTS mt1 SYNC")
    node2.query("DROP TABLE IF EXISTS mt1 SYNC")

    try:
        for node in [node1, node2]:
            node.query(
                f"CREATE TABLE mt1 (id UInt64) ENGINE ReplicatedMergeTree('/clickhouse/tables/t1', '{node.name}') ORDER BY tuple() "
                "SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1, cleanup_thread_preferred_points_per_iteration=0,"
                "merge_selecting_sleep_ms=100, max_merge_selecting_sleep_ms=1000"
            )

        node1.query("SYSTEM STOP MERGES mt1")
        node2.query("SYSTEM STOP REPLICATION QUEUES")

        for i in range(5):
            node1.query(f"INSERT INTO mt1 VALUES ({i})")

        for i in range(20):
            parts_to_merge = node1.query(
                "SELECT parts_to_merge FROM system.replication_queue WHERE table='mt1' AND length(parts_to_merge) > 0"
            )
            if parts_to_merge:
                parts_list = list(sorted(ast.literal_eval(parts_to_merge)))
                print("Got parts list", parts_list)
                if len(parts_list) < 3:
                    raise Exception("Got too small parts list {}".format(parts_list))
                break
            time.sleep(1)

        victim_part_from_the_middle = random.choice(parts_list[1:-1])
        print("Will corrupt part", victim_part_from_the_middle)

        remove_part_from_disk(node1, "mt1", victim_part_from_the_middle)

        # other way to detect broken parts
        node1.query("CHECK TABLE mt1")

        node2.query("SYSTEM START REPLICATION QUEUES")
        # Reduce timeout in sync replica since it might never finish with merge stopped and we don't want to wait 300s
        res, err = node1.query_and_get_answer_with_error(
            "SYSTEM SYNC REPLICA mt1", settings={"receive_timeout": 30}
        )
        print("result: ", res)
        print("error: ", res)

        for i in range(10):
            result = node2.query("SELECT count() FROM system.replication_queue")
            if int(result) == 0:
                break
            time.sleep(1)
        else:
            assert False, "Still have something in replication queue:\n" + node2.query(
                "SELECT * FROM system.replication_queue FORMAT Vertical"
            )

        assert node1.contains_in_log(
            f"Created empty part {victim_part_from_the_middle}"
        ) or node1.contains_in_log(
            f"Part {victim_part_from_the_middle} looks broken. Removing it and will try to fetch."
        ), f"Seems like empty part {victim_part_from_the_middle} is not created or log message changed"

        assert_eq_with_retry(node2, "SELECT COUNT() FROM mt1", "4")
        assert_eq_with_retry(node2, "SELECT COUNT() FROM system.replication_queue", "0")

        node1.query("SYSTEM START MERGES mt1")

        assert_eq_with_retry(node1, "SELECT COUNT() FROM mt1", "4")
        assert_eq_with_retry(node1, "SELECT COUNT() FROM system.replication_queue", "0")
    finally:
        node1.query("DROP TABLE IF EXISTS mt1 SYNC")
        node2.query("DROP TABLE IF EXISTS mt1 SYNC")


def test_lost_part_mutation(start_cluster):
    node1.query("DROP TABLE IF EXISTS mt2 SYNC")
    node2.query("DROP TABLE IF EXISTS mt2 SYNC")

    try:
        for node in [node1, node2]:
            node.query(
                f"CREATE TABLE mt2 (id UInt64) ENGINE ReplicatedMergeTree('/clickhouse/tables/t2', '{node.name}') ORDER BY tuple() "
                "SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1, cleanup_thread_preferred_points_per_iteration=0,"
                "merge_selecting_sleep_ms=100, max_merge_selecting_sleep_ms=1000"
            )

        node1.query("SYSTEM STOP MERGES mt2")
        node2.query("SYSTEM STOP REPLICATION QUEUES")

        for i in range(2):
            node1.query(f"INSERT INTO mt2 VALUES ({i})")

        node1.query(
            "ALTER TABLE mt2 UPDATE id = 777 WHERE 1", settings={"mutations_sync": "0"}
        )

        for i in range(20):
            parts_to_mutate = node1.query(
                "SELECT count() FROM system.replication_queue WHERE table='mt2'"
            )
            # two mutations for both replicas
            if int(parts_to_mutate) == 4:
                break
            time.sleep(1)

        remove_part_from_disk(node1, "mt2", "all_1_1_0")

        # other way to detect broken parts
        node1.query("CHECK TABLE mt2")

        node1.query("SYSTEM START MERGES mt2")
        res, err = node1.query_and_get_answer_with_error("SYSTEM SYNC REPLICA mt2")
        print("result: ", res)
        print("error: ", res)

        for i in range(10):
            result = node1.query("SELECT count() FROM system.replication_queue")
            if int(result) == 0:
                break
            time.sleep(1)
        else:
            assert False, "Still have something in replication queue:\n" + node1.query(
                "SELECT * FROM system.replication_queue FORMAT Vertical"
            )

        assert_eq_with_retry(node1, "SELECT COUNT() FROM mt2", "1")
        assert_eq_with_retry(node1, "SELECT SUM(id) FROM mt2", "777")
        assert_eq_with_retry(node1, "SELECT COUNT() FROM system.replication_queue", "0")

        node2.query("SYSTEM START REPLICATION QUEUES")

        assert_eq_with_retry(node2, "SELECT COUNT() FROM mt2", "1")
        assert_eq_with_retry(node2, "SELECT SUM(id) FROM mt2", "777")
        assert_eq_with_retry(node2, "SELECT COUNT() FROM system.replication_queue", "0")
    finally:
        node1.query("DROP TABLE IF EXISTS mt2 SYNC")
        node2.query("DROP TABLE IF EXISTS mt2 SYNC")


def test_lost_last_part(start_cluster):
    node1.query("DROP TABLE IF EXISTS mt3 SYNC")
    node2.query("DROP TABLE IF EXISTS mt3 SYNC")

    try:
        for node in [node1, node2]:
            node.query(
                f"CREATE TABLE mt3 (id UInt64, p String) ENGINE ReplicatedMergeTree('/clickhouse/tables/t3', '{node.name}') "
                "ORDER BY tuple() PARTITION BY p SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1, cleanup_thread_preferred_points_per_iteration=0,"
                "merge_selecting_sleep_ms=100, max_merge_selecting_sleep_ms=1000"
            )

        node1.query("SYSTEM STOP MERGES mt3")
        node2.query("SYSTEM STOP REPLICATION QUEUES")

        for i in range(1):
            node1.query(f"INSERT INTO mt3 VALUES ({i}, 'x')")

        # actually not important
        node1.query(
            "ALTER TABLE mt3 UPDATE id = 777 WHERE 1", settings={"mutations_sync": "0"}
        )

        partition_id = node1.query("select partitionID('x')").strip()
        remove_part_from_disk(node1, "mt3", f"{partition_id}_0_0_0")

        # other way to detect broken parts
        node1.query("CHECK TABLE mt3")

        node1.query("SYSTEM START MERGES mt3")

        for i in range(100):
            result = node1.query(
                "SELECT count() FROM system.replication_queue WHERE table='mt3'"
            )
            assert int(result) <= 2, "Have a lot of entries in queue {}".format(
                node1.query("SELECT * FROM system.replication_queue FORMAT Vertical")
            )
            if node1.contains_in_log(
                "Cannot create empty part"
            ) and node1.contains_in_log("DROP/DETACH PARTITION"):
                break
            if node1.contains_in_log(
                "Created empty part 8b8f0fede53df97513a9fb4cb19dc1e4_0_0_0 "
            ):
                break
            time.sleep(0.5)
        else:
            assert False, "Don't have required messages in node1 log"

        node1.query(f"ALTER TABLE mt3 DROP PARTITION ID '{partition_id}'")

        assert_eq_with_retry(node1, "SELECT COUNT() FROM mt3", "0")
        assert_eq_with_retry(node1, "SELECT COUNT() FROM system.replication_queue", "0")
    finally:
        node1.query("DROP TABLE IF EXISTS mt3 SYNC")
        node2.query("DROP TABLE IF EXISTS mt3 SYNC")
