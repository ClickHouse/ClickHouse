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
                + f" SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 60"
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
                + f"SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 2;"
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
                + f" SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 100, geo_replication_control_leader_election_period_ms = 1000,"
                + f" fetch_covered_part_within_region_only = 1"
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
                + f" SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 100, fetch_merged_part_within_region_only = 1,"
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


def test_only_fetch_covered_part_from_same_region(start_cluster):
    try:
        for i, node in enumerate(
            [apac1, apac2, us3]
        ):  # apac1 will become leader of APAC
            node.query(
                f"CREATE TABLE us_table(key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/us_table', '{i}') ORDER BY tuple() PARTITION BY key"
                + f" SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 100"
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
            apac2.query(
                "SELECT count() FROM system.parts WHERE database = 'default' AND table = 'us_table' AND active"
            )
        )

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
