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

        # restart apac1, so apac2 should becomes leader and will start fetching from apac1
        cluster.restart_instance(apac1)

        # limit send speed from us3 to 1 bytes/s, so no replica can successfully fetch from it
        us3.query(
            "ALTER TABLE us_table MODIFY SETTING max_replicated_sends_network_bandwidth = 1"
        )

        us3_send_speed = []
        apac2.query("SYSTEM START FETCHES us_table")

        time.sleep(5)

        # apac2 should fetch from apac1 have full data now
        count_ref = int(us3.query("SELECT count() FROM us_table"))
        count = int(apac2.query("SELECT count() FROM us_table"))
        assert (
            count == count_ref
        ), "Follower should start fetching from any replica if leader timeout, but table on follower is empty"

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
                + f"SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 1;"
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
                + f" SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 100, geo_replication_control_leader_election_period_ms = 1000;"
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
                    apac1.query("SELECT count() FROM system.replicas WHERE is_readonly")
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
                + f" SETTINGS geo_replication_control_leader_wait = 1, geo_replication_control_leader_wait_timeout = 100, fetch_merged_part_within_region_only = 1"
            )
            time.sleep(1)

        apac1.query("ALTER TABLE us_table MODIFY SETTING always_fetch_merged_part = 1")
        apac2.query("ALTER TABLE us_table MODIFY SETTING always_fetch_merged_part = 1")
        us3.query("SYSTEM STOP MERGES us_table")

        for i in range(5):
            us3.query("INSERT INTO us_table SELECT 1, toString({})".format(i))

        us3.query("SYSTEM START MERGES us_table")
        us3.query("OPTIMIZE TABLE us_table FINAL")

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
                ), "After 60s, all nodes in apac should have same data but count() on each replica is still inconsistent, apac1: {}, apac2: {}".format()

        num_part1 = int(
            apac1.query(
                "SELECT count() FROM system.parts WHERE database = 'default' AND table = 'us_table' AND active"
            )
        )
        num_part2 = int(
            apac2.query(
                "SELECT count() FROM system.parts WHERE database = 'default' AND table = 'us_table' AND active"
            )
        )

        assert (
            num_part1 == 5
        ), "APAC1 should not fetched merged part from US and has 5 part, but got {}".format(
            num_part1
        )
        assert (
            num_part2 == 5
        ), "APAC2 should not fetched merged part from US and has 5 part, but got {}".format(
            num_part2
        )

    finally:
        for node in [apac1, apac2, us3]:
            node.query("DROP TABLE IF EXISTS us_table SYNC")
