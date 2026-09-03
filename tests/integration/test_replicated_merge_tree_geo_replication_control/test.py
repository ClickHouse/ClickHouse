#!/usr/bin/env python3
from helpers.cluster import ClickHouseCluster
import pytest
import random
import string
import time

cluster = ClickHouseCluster(__file__)
apac1 = cluster.add_instance(
    "apac1", main_configs=["configs/apac1.xml"], with_zookeeper=True
)
apac2 = cluster.add_instance(
    "apac2", main_configs=["configs/apac2.xml"], with_zookeeper=True
)
us3 = cluster.add_instance("us3", main_configs=["configs/us3.xml"], with_zookeeper=True)
us4 = cluster.add_instance("us4", main_configs=["configs/us4.xml"], with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def get_random_string(length):
    return "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(length)
    )


def test_follower_only_fetch_from_leader(start_cluster):
    try:
        for i, node in enumerate(
            [apac1, apac2, us3]
        ):  # apac1 will become leader of APAC
            node.query(
                f"CREATE TABLE us_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/us_table', '{i}') ORDER BY tuple() PARTITION BY key"
                + " SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 60"
            )
            time.sleep(1)

        apac1.query("SYSTEM STOP FETCHES us_table")
        apac2.query("SYSTEM STOP FETCHES us_table")

        for i in range(5):
            us3.query("INSERT INTO us_table SELECT 1, toString({})".format(i))

        apac2.query("SYSTEM START FETCHES us_table")

        time.sleep(5)

        # apac2 should have no data leader apac1 doesn't fetch
        count = int(apac2.query("SELECT count() FROM us_table"))
        assert count == 0, (
            "Follower shouldn't have any data be cause the the region leader doesn't fetch, but have "
            + str(count)
            + " rows"
        )

        # stop fetch from apac2 and start fetching from apac1
        apac2.query("SYSTEM STOP FETCHES us_table")
        apac1.query("SYSTEM START FETCHES us_table")
        apac1.query("SYSTEM SYNC REPLICA us_table LIGHTWEIGHT")
        us3.query("SYSTEM STOP REPLICATED SENDS")

        # restart apac1, so apac2 should becomes leader and will start fetching from apac1
        cluster.restart_instance(apac1)

        apac2.query("SYSTEM START FETCHES us_table")

        time.sleep(5)

        # apac2 should fetch from apac1 have full data now
        count_ref = int(us3.query("SELECT count() FROM us_table"))
        count = int(apac2.query("SELECT count() FROM us_table"))
        assert (
            count == count_ref
        ), "Apac2 should becomes leader and fetches from apac1, but table on apac2 only has {} rows compared to {} rows on apac1".format(
            count_ref, count
        )

    finally:
        for node in [apac1, apac2, us3]:
            node.query("DROP TABLE IF EXISTS us_table SYNC")


def test_follower_fetch_from_leader_timeout(start_cluster):
    try:
        for i, node in enumerate(
            [apac1, apac2, us3]
        ):  # apac1 will become leader of APAC
            node.query(
                f"CREATE TABLE us_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/us_table', '{i}') ORDER BY tuple() PARTITION BY key "
                + "SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 2;"
            )
            time.sleep(1)

        apac1.query("SYSTEM STOP FETCHES us_table")

        for i in range(5):
            us3.query("INSERT INTO us_table SELECT 1, toString({})".format(i))

        # apac2 waited for too long and should start fetch from us3
        apac2.query("SYSTEM SYNC REPLICA us_table LIGHTWEIGHT")

        count_ref = int(us3.query("SELECT count() FROM us_table"))
        count = int(apac2.query("SELECT count() FROM us_table"))
        assert (
            count == count_ref
        ), "Follower should start fetching from any replica if leader timeout, but table on follower is empty"

    finally:
        for node in [apac1, apac2, us3]:
            node.query("DROP TABLE IF EXISTS us_table SYNC")


def test_all_nodes_have_data_when_zookeeper_restart(start_cluster):
    try:
        for i, node in enumerate(
            [apac1, apac2, us3]
        ):  # apac1 will become leader of APAC
            node.query(
                f"CREATE TABLE us_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/us_table', '{i}') ORDER BY tuple() PARTITION BY key"
                + " SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 100, geo_replication_control_leader_election_period_ms = 1000,"
                + " fetch_covered_part_within_region_only = 1"
            )
            time.sleep(1)

        apac1.query("SYSTEM STOP FETCHES us_table")
        apac2.query("SYSTEM STOP FETCHES us_table")

        for i in range(5):
            us3.query("INSERT INTO us_table SELECT 1, toString({})".format(i))

        cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
        cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])

        apac1.query("SYSTEM START FETCHES us_table")
        apac2.query("SYSTEM START FETCHES us_table")

        # wait all node to be ready
        while 1:
            time.sleep(0.5)
            if (
                int(
                    apac1.query("SELECT count() FROM system.replicas WHERE is_readonly")
                )
                == 0
                and int(
                    apac2.query("SELECT count() FROM system.replicas WHERE is_readonly")
                )
                == 0
            ):
                break

        # we don't care who is the leader, but all nodes must have data
        count_ref = int(us3.query("SELECT count() FROM us_table"))

        timeout = 60.0  # should be more than enough to fetch all
        now = time.time()
        while 1:
            time.sleep(0.5)
            count1 = int(apac1.query("SELECT count() FROM us_table"))
            count2 = int(apac2.query("SELECT count() FROM us_table"))
            if count1 == count_ref and count2 == count_ref:
                break

            if time.time() - now > timeout:
                assert (
                    False
                ), "After 60s, all nodes in apac should have same data but count() on each replica is still inconsistent, apac1: {}, apac2: {}".format(
                    count1, count2
                )

    finally:
        for node in [apac1, apac2, us3]:
            node.query("DROP TABLE IF EXISTS us_table SYNC")


def test_merged_cannot_fetch_across_regions(start_cluster):
    try:
        for i, node in enumerate(
            [apac1, apac2, us3]
        ):  # apac1 will become leader of APAC
            node.query(
                f"CREATE TABLE us_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/us_table', '{i}') ORDER BY tuple() PARTITION BY key"
                + " SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 100, fetch_merged_part_within_region_only = 1,"
                + " always_fetch_merged_part = {}".format(int(node != apac1))
            )
            time.sleep(1)

        for i in range(5):
            apac1.query("INSERT INTO us_table SELECT 1, toString({})".format(i))

        apac1.query("OPTIMIZE TABLE us_table FINAL")

        count_ref = int(apac1.query("SELECT count() FROM us_table"))
        timeout = 60.0  # should be more than enough to fetch all
        now = time.time()
        while 1:
            time.sleep(0.5)
            count2 = int(apac2.query("SELECT count() FROM us_table"))
            count3 = int(us3.query("SELECT count() FROM us_table"))
            if count2 == count_ref and count3 == count_ref:
                break

            if time.time() - now > timeout:
                assert (
                    False
                ), "After 60s, all nodes should have same data but count() on each replica is still inconsistent, us3: {}, apac2: {}".format(
                    count3, count2
                )

        num_part1 = int(
            apac1.query(
                "SELECT count() FROM system.parts WHERE database = 'default' AND table = 'us_table' AND active"
            )
        )
        apac2.query("SYSTEM SYNC REPLICA us_table")
        num_part2 = int(
            apac2.query(
                "SELECT count() FROM system.parts WHERE database = 'default' AND table = 'us_table' AND active"
            )
        )
        num_part3 = int(
            us3.query(
                "SELECT count() FROM system.parts WHERE database = 'default' AND table = 'us_table' AND active"
            )
        )

        assert (
            num_part2 == num_part1
        ), "APAC2 should fetched merged part from APAC1 and has same number of parts, apac1: {}, apac2: {}".format(
            num_part1, num_part2
        )
        assert (
            num_part3 == 5
        ), "US3 should not fetched merged part from APAC1 and has 5 part, but got {}".format(
            num_part3
        )

    finally:
        for node in [apac1, apac2, us3]:
            node.query("DROP TABLE IF EXISTS us_table SYNC")


def test_region_setting_is_readonly_after_creation(start_cluster):
    try:
        apac1.query(
            "CREATE TABLE us_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/us_table', '0') ORDER BY tuple() PARTITION BY key"
        )

        # The geo controller snapshots the region once in its constructor and creates its background task there,
        # so changing the region on a live table is rejected instead of silently leaving the controller in its
        # old state until the next restart.
        assert "READONLY_SETTING" in apac1.query_and_get_error(
            "ALTER TABLE us_table MODIFY SETTING geo_replication_control_region = 'US'"
        )

        # The other geo settings are read live at each fetch / election, so they remain alterable.
        # `system.merge_tree_settings` only exposes the server-wide defaults, so check the table metadata.
        apac1.query(
            "ALTER TABLE us_table MODIFY SETTING geo_replication_control_leader_wait = 7"
        )
        assert (
            "geo_replication_control_leader_wait = 7"
            in apac1.query(
                "SELECT engine_full FROM system.tables WHERE database = 'default' AND name = 'us_table'"
            )
        )

    finally:
        apac1.query("DROP TABLE IF EXISTS us_table SYNC")


def test_region_published_on_restart(start_cluster):
    try:
        for i, node in enumerate([apac1, apac2, us3]):  # apac1 will become leader of APAC
            node.query(
                f"CREATE TABLE us_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/us_table', '{i}') ORDER BY tuple() PARTITION BY key"
                + " SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 60"
            )
            time.sleep(1)

        # Restart a follower. Its region membership node `/replicas/<name>/region` is published synchronously as
        # part of startup, before the replication queue is activated, so peers never misclassify a recovering
        # replica as out-of-region and it never falls back to a cross-region fetch during recovery.
        cluster.restart_instance(apac2)

        timeout = 60.0
        now = time.time()
        while 1:
            region = apac2.query(
                "SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/us_table/replicas/1' AND name = 'region'"
            ).strip()
            if region == "APAC":
                break
            assert (
                time.time() - now < timeout
            ), "apac2 did not publish its region node after restart, got '{}'".format(
                region
            )
            time.sleep(0.5)

    finally:
        for node in [apac1, apac2, us3]:
            node.query("DROP TABLE IF EXISTS us_table SYNC")


def test_region_published_before_is_active(start_cluster):
    try:
        for i, node in enumerate(
            [apac1, apac2, us3]
        ):  # apac1 will become leader of APAC
            node.query(
                f"CREATE TABLE us_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/us_table', '{i}') ORDER BY tuple() PARTITION BY key"
                + " SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 60"
            )
            time.sleep(1)

        cluster.restart_instance(apac2)

        # Wait until the restarted replica has fully started up and recreated both znodes.
        timeout = 60.0
        now = time.time()
        while int(apac2.query("SELECT count() FROM system.replicas WHERE is_readonly")):
            assert time.time() - now < timeout, "apac2 is still readonly after restart"
            time.sleep(0.5)

        # The region node must be created before `is_active`: peers pick fetch sources among active replicas
        # and classify them by the `region` node, so an `is_active` without `region` would make peers
        # misclassify this replica as out-of-region for the whole startup window. ZooKeeper czxid gives the
        # global creation order.
        czxids = {}
        for name in ["region", "is_active"]:
            czxids[name] = int(
                us3.query(
                    f"SELECT czxid FROM system.zookeeper WHERE path = '/clickhouse/tables/us_table/replicas/1' AND name = '{name}'"
                )
            )
        assert (
            czxids["region"] < czxids["is_active"]
        ), "The region node must be published before is_active, got czxids {}".format(
            czxids
        )

    finally:
        for node in [apac1, apac2, us3]:
            node.query("DROP TABLE IF EXISTS us_table SYNC")


def test_quorum_get_part_does_not_bypass_region(start_cluster):
    try:
        for i, node in enumerate(
            [apac1, apac2, us3, us4]
        ):  # apac1 will become leader of APAC, us3 of US
            node.query(
                f"CREATE TABLE us_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/us_table', '{i}') ORDER BY tuple() PARTITION BY key"
                + " SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 60"
            )
            time.sleep(1)

        # The APAC leader must not have the part, so the APAC follower's same-region probe fails and its
        # quorum GET_PART handling has to decide about the out-of-region holders.
        apac1.query("SYSTEM STOP FETCHES us_table")

        # The quorum is reached within US (us3 + us4), so the part exists only out of the APAC region.
        us3.query("INSERT INTO us_table SELECT 1, '0'", settings={"insert_quorum": 2})

        time.sleep(5)

        # The quorum branch of the fetch re-probes all replicas (the quorum must not be marked as failed
        # while the part exists somewhere), but it must not use an out-of-region replica as a fetch source:
        # the APAC follower has to keep deferring instead of fetching cross-region.
        count = int(apac2.query("SELECT count() FROM us_table"))
        assert (
            count == 0
        ), "Follower fetched a quorum part cross-region, but it should defer, got {} rows".format(
            count
        )

        # ... and the quorum must not have been marked as failed either.
        failed_parts = apac2.query(
            "SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/us_table/quorum/failed_parts'"
        ).strip()
        assert (
            failed_parts == ""
        ), "Quorum was marked as failed although the part exists: {}".format(
            failed_parts
        )

        # Once the region leader may fetch, the part propagates: leader cross-region, follower from the leader.
        apac1.query("SYSTEM START FETCHES us_table")

        count_ref = int(us3.query("SELECT count() FROM us_table"))
        timeout = 60.0
        now = time.time()
        while 1:
            time.sleep(0.5)
            count = int(apac2.query("SELECT count() FROM us_table"))
            if count == count_ref:
                break
            assert (
                time.time() - now < timeout
            ), "After 60s the follower still has {} rows instead of {}".format(
                count, count_ref
            )

    finally:
        for node in [apac1, apac2, us3, us4]:
            node.query("DROP TABLE IF EXISTS us_table SYNC")


def test_merged_part_fetch_is_postponed_not_spinning(start_cluster):
    try:
        for i, node in enumerate(
            [apac1, apac2, us3]
        ):  # apac1 will become leader of APAC, us3 of US
            node.query(
                f"CREATE TABLE us_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/us_table', '{i}') ORDER BY tuple() PARTITION BY key"
                + " SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 60, fetch_merged_part_within_region_only = 1,"
                + " always_fetch_merged_part = {}".format(int(node == apac2))
            )
            time.sleep(1)

        for i in range(5):
            apac1.query("INSERT INTO us_table SELECT 1, toString({})".format(i))

        # No APAC replica can produce the merged part: apac1's merges are stopped and apac2 always fetches
        # merged parts. The merge is executed by us3, out of the region.
        apac1.query("SYSTEM STOP MERGES us_table")
        us3.query("SYSTEM SYNC REPLICA us_table LIGHTWEIGHT")
        us3.query("OPTIMIZE TABLE us_table FINAL")

        # The region-constrained merged-part fetch must install a wait state so the queue postpones it,
        # instead of reselecting the entry immediately in a tight loop at the head of the queue.
        timeout = 30.0
        now = time.time()
        while 1:
            postpone_reason = apac2.query(
                "SELECT postpone_reason FROM system.replication_queue WHERE type = 'MERGE_PARTS'"
            )
            if "region leader may not be ready" in postpone_reason:
                break
            assert (
                time.time() - now < timeout
            ), "The merged-part fetch was not postponed with the region wait reason, got: {}".format(
                postpone_reason
            )
            time.sleep(0.5)

        # Once an in-region replica produces the merged part, the postponed entry completes within the region.
        apac1.query("SYSTEM START MERGES us_table")

        num_parts_ref = 1
        timeout = 60.0
        now = time.time()
        while 1:
            time.sleep(0.5)
            num_parts = int(
                apac2.query(
                    "SELECT count() FROM system.parts WHERE database = 'default' AND table = 'us_table' AND active"
                )
            )
            if num_parts == num_parts_ref:
                break
            assert (
                time.time() - now < timeout
            ), "After 60s apac2 still has {} active parts instead of {}".format(
                num_parts, num_parts_ref
            )

    finally:
        for node in [apac1, apac2, us3]:
            node.query("DROP TABLE IF EXISTS us_table SYNC")


def test_only_fetch_covered_part_from_same_region(start_cluster):
    try:
        for i, node in enumerate(
            [apac1, apac2, us3]
        ):  # apac1 will become leader of APAC
            node.query(
                f"CREATE TABLE us_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/us_table', '{i}') ORDER BY tuple() PARTITION BY key"
                + " SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 100"
            )
            time.sleep(1)

        apac2.query("SYSTEM STOP REPLICATION QUEUES us_table")
        us3.query("SYSTEM STOP REPLICATION QUEUES us_table")

        for i in range(5):
            apac1.query("INSERT INTO us_table SELECT 1, toString({})".format(i))

        apac1.query("OPTIMIZE TABLE us_table FINAL")

        apac2.query("SYSTEM STOP MERGES us_table")
        us3.query("SYSTEM STOP MERGES us_table")

        apac2.query("SYSTEM START REPLICATION QUEUES us_table")
        us3.query("SYSTEM START REPLICATION QUEUES us_table")

        count_ref = int(apac1.query("SELECT count() FROM us_table"))
        timeout = 60.0  # should be more than enough to fetch all
        now = time.time()
        while 1:
            time.sleep(0.5)
            count2 = int(apac2.query("SELECT count() FROM us_table"))
            count3 = int(us3.query("SELECT count() FROM us_table"))
            if count2 == count_ref and count3 == count_ref:
                break

            if time.time() - now > timeout:
                assert (
                    False
                ), "After 60s, all nodes should have same data but count() on each replica is still inconsistent, us3: {}, apac2: {}".format(
                    count3, count2
                )

        num_part1 = int(
            apac1.query(
                "SELECT count() FROM system.parts WHERE database = 'default' AND table = 'us_table' AND active"
            )
        )

        # Only wait for the fetches: merges are stopped on this replica, so the `MERGE_PARTS` entry produced by
        # `OPTIMIZE` stays in the queue and a full `SYSTEM SYNC REPLICA` would wait for it until the timeout.
        apac2.query("SYSTEM SYNC REPLICA us_table LIGHTWEIGHT")
        num_part2 = int(
            apac2.query(
                "SELECT count() FROM system.parts WHERE database = 'default' AND table = 'us_table' AND active"
            )
        )
        num_part3 = int(
            us3.query(
                "SELECT count() FROM system.parts WHERE database = 'default' AND table = 'us_table' AND active"
            )
        )

        assert (
            num_part2 == num_part1
        ), "APAC2 should fetched covered part from APAC1 and has same number of parts, apac1: {}, apac2: {}".format(
            num_part1, num_part2
        )
        assert (
            num_part3 == 5
        ), "US3 should not fetched merged part from APAC1 and has 5 part, but got {}".format(
            num_part3
        )

    finally:
        for node in [apac1, apac2, us3]:
            node.query("DROP TABLE IF EXISTS us_table SYNC")


def test_fetch_partition_prefers_same_region(start_cluster):
    src_path = "/clickhouse/tables/fetch_partition_src"
    try:
        for node, replica in [(apac2, "apac2"), (us3, "us3"), (us4, "us4")]:
            node.query(
                f"CREATE TABLE src_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('{src_path}', '{replica}') ORDER BY tuple() PARTITION BY key"
                + " SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 60"
            )
            time.sleep(1)

        apac1.query(
            "CREATE TABLE dst_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/fetch_partition_dst', 'apac1') ORDER BY tuple() PARTITION BY key"
        )

        us3.query("INSERT INTO src_table SELECT 1, '0'")
        for node in [apac2, us3, us4]:
            node.query("SYSTEM SYNC REPLICA src_table LIGHTWEIGHT")

        # Make the out-of-region replicas strictly "better" by the log pointer / queue size criteria, so that
        # the same-region replica is chosen only if the region preference is actually honored.
        apac2.query("SYSTEM STOP PULLING REPLICATION LOG src_table")
        for i in range(5):
            us3.query(f"INSERT INTO src_table SELECT 2, toString({i})")
        us4.query("SYSTEM SYNC REPLICA src_table LIGHTWEIGHT")

        log_pointers = {
            node.name: int(
                node.query(
                    "SELECT log_pointer FROM system.replicas WHERE table = 'src_table'"
                )
            )
            for node in [apac2, us3, us4]
        }
        assert (
            log_pointers["apac2"] < log_pointers["us3"]
            and log_pointers["apac2"] < log_pointers["us4"]
        ), "The same-region replica must lag behind for this test to be meaningful, got {}".format(
            log_pointers
        )

        apac1.query(f"ALTER TABLE dst_table FETCH PARTITION 1 FROM '{src_path}'")

        assert apac1.contains_in_log(
            "Selected apac2 to fetch from"
        ), "FETCH PARTITION did not choose the same-region replica"
        for replica in ["us3", "us4"]:
            assert not apac1.contains_in_log(
                f"Selected {replica} to fetch from"
            ), f"FETCH PARTITION chose the out-of-region replica {replica}"

        detached = int(
            apac1.query(
                "SELECT count() FROM system.detached_parts WHERE table = 'dst_table'"
            )
        )
        assert detached == 1, "Expected one detached part, got {}".format(detached)

    finally:
        for node in [apac2, us3, us4]:
            node.query("DROP TABLE IF EXISTS src_table SYNC")
        apac1.query("DROP TABLE IF EXISTS dst_table SYNC")


def test_fetch_partition_falls_back_when_same_region_has_no_partition(start_cluster):
    src_path = "/clickhouse/tables/fetch_partition_fallback_src"
    try:
        for node, replica in [(apac2, "apac2"), (us3, "us3")]:
            node.query(
                f"CREATE TABLE fallback_src(key UInt64, data String) ENGINE = ReplicatedMergeTree('{src_path}', '{replica}') ORDER BY tuple() PARTITION BY key"
                + " SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 60"
            )
            time.sleep(1)

        apac1.query(
            "CREATE TABLE fallback_dst(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/fetch_partition_fallback_dst', 'apac1') ORDER BY tuple() PARTITION BY key"
        )

        # The same-region replica stays active, but never gets the data of the partition to fetch.
        apac2.query("SYSTEM STOP FETCHES fallback_src")
        us3.query("INSERT INTO fallback_src SELECT 1, '0'")
        us3.query("SYSTEM SYNC REPLICA fallback_src LIGHTWEIGHT")

        assert (
            int(
                apac2.query(
                    "SELECT count() FROM system.parts WHERE table = 'fallback_src' AND active"
                )
            )
            == 0
        ), "The same-region replica must not have the partition for this test to be meaningful"

        # A user query is never deferred, so an out-of-region replica has to be used here.
        apac1.query(f"ALTER TABLE fallback_dst FETCH PARTITION 1 FROM '{src_path}'")

        detached = int(
            apac1.query(
                "SELECT count() FROM system.detached_parts WHERE table = 'fallback_dst'"
            )
        )
        assert detached == 1, "Expected one detached part, got {}".format(detached)

    finally:
        for node in [apac2, us3]:
            node.query("DROP TABLE IF EXISTS fallback_src SYNC")
        apac1.query("DROP TABLE IF EXISTS fallback_dst SYNC")


def test_clone_part_from_shard_skips_lagging_same_region_replica(start_cluster):
    src_shard = "/clickhouse/tables/clone_from_shard_src"
    dst_shard = "/clickhouse/tables/clone_from_shard_dst"
    move_settings = (
        " SETTINGS part_moves_between_shards_enable = 1, part_moves_between_shards_delay_seconds = 0,"
        " assign_part_uuids = 1, geo_replication_control_leader_wait = 1,"
        " geo_replication_control_leader_wait_timeout = 60"
    )
    try:
        for node, replica in [(apac2, "apac2"), (us3, "us3")]:
            node.query(
                f"CREATE TABLE clone_shard(key UInt64, data String) ENGINE = ReplicatedMergeTree('{src_shard}', '{replica}') ORDER BY tuple() PARTITION BY key"
                + move_settings
            )
            time.sleep(1)

        apac1.query(
            f"CREATE TABLE clone_shard(key UInt64, data String) ENGINE = ReplicatedMergeTree('{dst_shard}', 'apac1') ORDER BY tuple() PARTITION BY key"
            + move_settings
        )

        # The replica of the destination region stays active, but never gets the part to clone.
        apac2.query("SYSTEM STOP FETCHES clone_shard")
        us3.query("INSERT INTO clone_shard SELECT 1, '0'")

        part_name = us3.query(
            "SELECT name FROM system.parts WHERE table = 'clone_shard' AND active"
        ).strip()

        us3.query(
            f"ALTER TABLE clone_shard MOVE PART '{part_name}' TO SHARD '{dst_shard}'"
        )

        # The destination replica prefers a source replica of its own region, but only among the replicas that
        # really have the part - otherwise the move would never complete.
        for _ in range(60):
            if int(apac1.query("SELECT count() FROM clone_shard")) == 1:
                break
            time.sleep(1)

        assert (
            int(apac1.query("SELECT count() FROM clone_shard")) == 1
        ), "The part was not cloned from the source shard: {}".format(
            apac1.query(
                "SELECT type, num_tries, last_exception FROM system.replication_queue WHERE table = 'clone_shard' FORMAT Vertical"
            )
        )

        assert apac1.contains_in_log(
            f"Will clone part from shard {src_shard} and replica us3"
        ), "The part was not cloned from the replica that has it"
        assert not apac1.contains_in_log(
            f"Will clone part from shard {src_shard} and replica apac2"
        ), "The part was cloned from the same-region replica that does not have it"

    finally:
        for node in [apac1, apac2, us3]:
            node.query("DROP TABLE IF EXISTS clone_shard SYNC")
