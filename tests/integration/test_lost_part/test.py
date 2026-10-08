#!/usr/bin/env python3

import ast
import random
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True, stay_alive=True)
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
                "merge_selecting_sleep_ms=100, max_merge_selecting_sleep_ms=1000,"
                "max_postpone_time_for_failed_replicated_fetches_ms=0, max_postpone_time_for_failed_replicated_merges_ms=0"
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
                "merge_selecting_sleep_ms=100, max_merge_selecting_sleep_ms=1000,"
                "max_postpone_time_for_failed_replicated_fetches_ms=0, max_postpone_time_for_failed_replicated_merges_ms=0"
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
                "merge_selecting_sleep_ms=100, max_merge_selecting_sleep_ms=1000, max_postpone_time_for_failed_mutations_ms = 0,"
                "max_postpone_time_for_failed_replicated_fetches_ms=0, max_postpone_time_for_failed_replicated_merges_ms=0"
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


def count_in_log(node, substring):
    # Number of matching lines, including rotated logs. contains_in_log answers "ever", which a
    # line left by an earlier run of the same test already satisfies; comparing this count across
    # an action keeps a check about that action.
    return len([line for line in node.grep_in_log(substring).splitlines() if line])


def test_lost_last_part_modulo_partition_key(start_cluster):
    # An empty part created instead of a lost one takes its partition value from the partition id,
    # so that value must be typed like the one an INSERT produces. For a `modulo` whose left operand
    # is unsigned and whose right operand is signed the two differ in signedness, and a partition
    # value of the wrong signedness cannot be addressed by a later partition manipulation.
    node1.query("DROP TABLE IF EXISTS mt_mod SYNC")
    node2.query("DROP TABLE IF EXISTS mt_mod SYNC")

    try:
        for node in [node1, node2]:
            node.query(
                f"CREATE TABLE mt_mod (c0 Int32) ENGINE ReplicatedMergeTree('/clickhouse/tables/t_mod', '{node.name}') "
                "ORDER BY tuple() PARTITION BY (37528 % c0) SETTINGS cleanup_delay_period=1, cleanup_delay_period_random_add=1,"
                "cleanup_thread_preferred_points_per_iteration=0, merge_selecting_sleep_ms=100, max_merge_selecting_sleep_ms=1000"
            )

        node1.query("SYSTEM STOP MERGES mt_mod")
        # The other replica must never receive the part, otherwise the lost part is fetched
        # instead of being replaced by an empty one.
        node2.query("SYSTEM STOP REPLICATION QUEUES")

        # Exactly one part: the partition must be left with no active part, or the empty part
        # copies its partition value from a sibling instead of parsing the partition id.
        node1.query(
            "INSERT INTO mt_mod VALUES (167682982)",
            settings={"insert_keeper_fault_injection_probability": 0},
        )

        part_name = node1.query(
            "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'mt_mod' AND active"
        ).strip()
        assert part_name.startswith("37528_"), part_name

        created = f"Created empty part {part_name} instead of lost part"
        # Either of these means the partition id was not parsed into a value, leaving nothing
        # for this test to exercise.
        unparsed = f"Empty part {part_name} is not created instead of lost part because there are no parts in partition"
        gave_up = f"Cannot create empty part {part_name} instead of lost"
        created_before = count_in_log(node1, created)
        unparsed_before = count_in_log(node1, unparsed)
        gave_up_before = count_in_log(node1, gave_up)

        remove_part_from_disk(node1, "mt_mod", part_name)

        node1.query("CHECK TABLE mt_mod")
        node1.query("SYSTEM START MERGES mt_mod")

        for _ in range(200):
            if count_in_log(node1, created) > created_before:
                break
            time.sleep(0.5)
        else:
            assert False, "Empty part was not created instead of lost part " + part_name

        assert count_in_log(node1, unparsed) == unparsed_before
        assert count_in_log(node1, gave_up) == gave_up_before

        # By value: the by-id spelling resolves the partition without parsing a value at all.
        node1.query("ALTER TABLE mt_mod DROP PARTITION 37528")

        assert_eq_with_retry(node1, "SELECT count() FROM mt_mod", "0")
    finally:
        node2.query("SYSTEM START REPLICATION QUEUES")
        node1.query("DROP TABLE IF EXISTS mt_mod SYNC")
        node2.query("DROP TABLE IF EXISTS mt_mod SYNC")


def remove_part_dir_from_disk(node, table, part_name):
    # Unlike remove_part_from_disk, removes the whole part directory,
    # so the part becomes missing rather than broken (empty).
    part_path = node.query(
        f"SELECT path FROM system.parts WHERE database = 'default' AND table = '{table}' AND name = '{part_name}'"
    ).strip()
    if not part_path:
        raise Exception("Part " + part_name + " doesn't exist")
    # ensure that path is absolute before removing
    assert part_path.startswith("/"), f"Path is relative: {part_path}"
    node.exec_in_container(
        ["bash", "-c", f"rm -rf {part_path}"], privileged=True, user="root"
    )


def assert_reads_without_logical_error(node, table):
    # Reading from a table with a lost part may fail, but it must not fail with LOGICAL_ERROR.
    for settings in [
        {},
        {"min_bytes_to_use_direct_io": 1, "local_filesystem_read_method": "pread_threadpool"},
    ]:
        res, err = node.query_and_get_answer_with_error(
            f"SELECT * FROM {table}", settings=settings
        )
        assert "LOGICAL_ERROR" not in res, res
        assert "LOGICAL_ERROR" not in err, err


def test_lost_part_intersecting_merges(start_cluster):
    # Converted from stateless test 02369_lost_part_intersecting_merges.
    table = "rmt_intersecting_merges"
    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node2.query(f"DROP TABLE IF EXISTS {table} SYNC")

    try:
        node1.query(
            f"CREATE TABLE {table} (n int) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{table}', '1') ORDER BY n"
        )
        node2.query(
            f"CREATE TABLE {table} (n int) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{table}', '2') ORDER BY n"
        )

        node1.query(
            f"INSERT INTO {table} VALUES (1)",
            settings={"insert_keeper_fault_injection_probability": 0},
        )
        node1.query(
            f"INSERT INTO {table} VALUES (2)",
            settings={"insert_keeper_fault_injection_probability": 0},
        )

        node1.query(f"SYSTEM SYNC REPLICA {table}")
        node2.query(f"SYSTEM SYNC REPLICA {table}")
        node2.query(f"SYSTEM STOP MERGES {table}")
        node1.query(f"OPTIMIZE TABLE {table} FINAL")

        assert (
            node1.query(f"SELECT 1, *, _part FROM {table} ORDER BY n")
            == "1\t1\tall_0_1_1\n1\t2\tall_0_1_1\n"
        )
        assert (
            node2.query(f"SELECT 2, *, _part FROM {table} ORDER BY n")
            == "2\t1\tall_0_0_0\n2\t2\tall_1_1_0\n"
        )

        remove_part_dir_from_disk(node1, table, "all_0_1_1")

        assert_reads_without_logical_error(node1, table)

        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")

        node1.query(
            f"INSERT INTO {table} VALUES (3)",
            settings={"insert_keeper_fault_injection_probability": 0},
        )
        node2.query(f"SYSTEM START MERGES {table}")
        node1.query(f"SYSTEM SYNC REPLICA {table}")
        node1.query(f"OPTIMIZE TABLE {table} FINAL")

        node1.query(f"SYSTEM SYNC REPLICA {table}")
        node2.query(f"SYSTEM SYNC REPLICA {table}")
        assert (
            node1.query(f"SELECT 3, *, _part FROM {table} ORDER BY n")
            == "3\t1\tall_0_2_2\n3\t2\tall_0_2_2\n3\t3\tall_0_2_2\n"
        )
        assert (
            node2.query(f"SELECT 4, *, _part FROM {table} ORDER BY n")
            == "4\t1\tall_0_2_2\n4\t2\tall_0_2_2\n4\t3\tall_0_2_2\n"
        )

        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_lost_part_intersecting_merges_aggressive_cleanup(start_cluster):
    # Converted from stateless test 02370_lost_part_intersecting_merges.
    table = "rmt_intersecting_merges_cleanup"
    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node2.query(f"DROP TABLE IF EXISTS {table} SYNC")

    try:
        node1.query(
            f"CREATE TABLE {table} (n int) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{table}', '1') ORDER BY n "
            "SETTINGS cleanup_delay_period=0, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0, old_parts_lifetime=0"
        )
        node2.query(
            f"CREATE TABLE {table} (n int) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{table}', '2') ORDER BY n"
        )

        node2.query(f"SYSTEM STOP REPLICATED SENDS {table}")
        node2.query(
            f"INSERT INTO {table} VALUES (0)",
            settings={"insert_keeper_fault_injection_probability": 0},
        )

        node1.query(
            f"INSERT INTO {table} VALUES (1)",
            settings={"insert_keeper_fault_injection_probability": 0},
        )
        node1.query(
            f"INSERT INTO {table} VALUES (2)",
            settings={"insert_keeper_fault_injection_probability": 0},
        )

        node1.query(f"SYSTEM SYNC REPLICA {table} PULL")

        # There's a stupid effect from "zero copy replication":
        # MERGE_PARTS all_1_2_1 can be executed by replica 2 even if it was assigned by replica 1
        # After that, replica 2 will not be able to execute that merge and will only try to fetch the part from replica 2
        # But sends are stopped on replica 2...
        start_sends_timer = threading.Timer(
            5, lambda: node2.query(f"SYSTEM START REPLICATED SENDS {table}")
        )
        start_sends_timer.start()
        try:
            node1.query(
                f"OPTIMIZE TABLE {table}", settings={"optimize_throw_if_noop": 1}
            )
            node1.query(f"SYSTEM SYNC REPLICA {table}")
        finally:
            start_sends_timer.join()

        assert (
            node1.query(f"SELECT 1, *, _part FROM {table} ORDER BY n")
            == "1\t0\tall_0_0_0\n1\t1\tall_1_2_1\n1\t2\tall_1_2_1\n"
        )

        remove_part_dir_from_disk(node1, table, "all_1_2_1")

        assert_reads_without_logical_error(node1, table)

        # Random sleep to vary timing relative to the (aggressive) cleanup thread,
        # same as "select sleep(0.1) from numbers($RANDOM % 30)" in the original test.
        time.sleep(0.1 * random.randint(0, 29))

        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")

        node1.query(
            f"INSERT INTO {table} VALUES (3)",
            settings={"insert_keeper_fault_injection_probability": 0},
        )
        node1.query(f"SYSTEM SYNC REPLICA {table} PULL")
        node1.query(f"OPTIMIZE TABLE {table} FINAL")

        node1.query(f"SYSTEM SYNC REPLICA {table}")
        assert (
            node1.query(f"SELECT 3, *, _part FROM {table} ORDER BY n")
            == "3\t0\tall_0_3_2\n3\t1\tall_0_3_2\n3\t2\tall_0_3_2\n3\t3\tall_0_3_2\n"
        )
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_missing_covered_part_on_start(start_cluster):
    # Converted from stateless test 04215_replicated_missing_covered_part_on_start.
    # The original emulated a server restart with "DETACH TABLE ... SYNC" + "ATTACH TABLE"
    # (the only option available to a stateless test); since the scenario is about part
    # loading on server startup, here we restart the server for real.
    table = "rmt_missing_covered_part"
    zk_path = f"/clickhouse/tables/{table}"
    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node2.query(f"DROP TABLE IF EXISTS {table} SYNC")

    try:
        node1.query(
            f"CREATE TABLE {table} (n int) ENGINE=ReplicatedMergeTree('{zk_path}', '1') ORDER BY n "
            "SETTINGS old_parts_lifetime=100500"
        )
        node2.query(
            f"CREATE TABLE {table} (n int) ENGINE=ReplicatedMergeTree('{zk_path}', '2') ORDER BY n "
            "SETTINGS old_parts_lifetime=100500"
        )

        node1.query(
            f"INSERT INTO {table} VALUES (1)",
            settings={"insert_keeper_fault_injection_probability": 0},
        )
        node1.query(
            f"INSERT INTO {table} VALUES (2)",
            settings={"insert_keeper_fault_injection_probability": 0},
        )

        node1.query(f"SYSTEM SYNC REPLICA {table}")
        node2.query(f"SYSTEM SYNC REPLICA {table}")
        node2.query(f"SYSTEM STOP MERGES {table}")
        node1.query(f"OPTIMIZE TABLE {table} FINAL")

        remove_part_dir_from_disk(node1, table, "all_0_1_1")

        # Read from the removed part must fail (query_and_get_error throws if it succeeds).
        node1.query_and_get_error(f"SELECT * FROM {table}")

        node1.restart_clickhouse()

        node1.query(
            f"INSERT INTO {table} VALUES (3)",
            settings={"insert_keeper_fault_injection_probability": 0},
        )
        node2.query(f"SYSTEM START MERGES {table}")
        node1.query(f"SYSTEM SYNC REPLICA {table}")
        node1.query(f"OPTIMIZE TABLE {table} FINAL")
        node1.query(f"SYSTEM SYNC REPLICA {table}")

        assert (
            int(
                node1.query(
                    f"SELECT count() FROM system.zookeeper WHERE path='{zk_path}/replicas/1/parts' AND name='all_0_1_1'"
                )
            )
            > 0
        ), "Missing all_0_1_1 in ZooKeeper"

        node1.restart_clickhouse()

        assert node1.query(f"SELECT count(), sum(n) FROM {table}") == "3\t6\n"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
