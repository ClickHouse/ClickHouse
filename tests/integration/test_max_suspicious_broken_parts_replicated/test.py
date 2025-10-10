#!/usr/bin/env python3
#
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import os

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True, with_zookeeper=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def break_part(table, part_name):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"rm /var/lib/clickhouse/data/default/{table}/{part_name}/columns.txt",
        ]
    )


def remove_part(table, part_name):
    node.exec_in_container(
        ["bash", "-c", f"rm -r /var/lib/clickhouse/data/default/{table}/{part_name}"]
    )


def get_count(table):
    return int(node.query(f"SELECT count() FROM {table}").strip())


def detach_table(table):
    node.query(f"DETACH TABLE {table}")


def attach_table(table):
    node.query(f"ATTACH TABLE {table}")


def remove_part_from_zookeeper(replica_path, part_name):
    zk = cluster.get_kazoo_client("zoo1")
    zk.delete(os.path.join(replica_path, f"parts/{part_name}"))


def test_unexpected_uncommitted_merge():
    node.query(
        """
    CREATE TABLE broken_table (key Int) ENGINE = ReplicatedMergeTree('/tables/broken', '1') ORDER BY tuple()
    SETTINGS max_suspicious_broken_parts = 0, replicated_max_ratio_of_wrong_parts=0"""
    )

    node.query("INSERT INTO broken_table SELECT number from numbers(10)")
    node.query("INSERT INTO broken_table SELECT number from numbers(10, 10)")

    node.query("OPTIMIZE TABLE broken_table FINAL")

    assert node.query("SELECT sum(key) FROM broken_table") == "190\n"
    assert (
        node.query(
            "SELECT name FROM system.parts where table = 'broken_table' and active"
        )
        == "all_0_1_1\n"
    )

    remove_part_from_zookeeper("/tables/broken/replicas/1", "all_0_1_1")

    detach_table("broken_table")
    attach_table("broken_table")

    # it's not readonly
    node.query("INSERT INTO broken_table SELECT 1")

    assert node.query("SELECT sum(key) FROM broken_table") == "191\n"
    assert (
        node.query(
            "SELECT name FROM system.parts where table = 'broken_table' and active order by name"
        )
        == "all_0_0_0\nall_1_1_0\nall_2_2_0\n"
    )


def test_unexpected_uncommitted_mutation():
    node.query(
        """
    CREATE TABLE broken_table0 (key Int) ENGINE = ReplicatedMergeTree('/tables/broken0', '1') ORDER BY tuple()
    SETTINGS max_suspicious_broken_parts = 0, replicated_max_ratio_of_wrong_parts=0, old_parts_lifetime=100500, sleep_before_loading_outdated_parts_ms=10000"""
    )

    node.query("INSERT INTO broken_table0 SELECT number from numbers(10)")

    node.query(
        "ALTER TABLE broken_table0 UPDATE key = key * 10 WHERE 1 SETTINGS mutations_sync=1"
    )

    assert node.query("SELECT sum(key) FROM broken_table0") == "450\n"
    assert (
        node.query(
            "SELECT name FROM system.parts where table = 'broken_table0' and active"
        )
        == "all_0_0_0_1\n"
    )

    remove_part_from_zookeeper("/tables/broken0/replicas/1", "all_0_0_0_1")

    detach_table("broken_table0")
    attach_table("broken_table0")

    node.query("INSERT INTO broken_table0 SELECT 1")

    # it may remain 45 if the nutation was finalized
    sum_key = node.query("SELECT sum(key) FROM broken_table0")
    assert sum_key == "46\n" or sum_key == "451\n"
    assert "all_0_0_0_1" in node.query(
        "SELECT name FROM system.detached_parts where table = 'broken_table0'"
    )


def test_corrupted_random_part():
    node.query(
        """
    CREATE TABLE broken_table_1 (key Int) ENGINE = ReplicatedMergeTree('/tables/broken_1', '1') ORDER BY tuple()
    SETTINGS max_suspicious_broken_parts = 0, replicated_max_ratio_of_wrong_parts=0"""
    )

    node.query("INSERT INTO broken_table_1 SELECT number from numbers(10)")
    node.query("INSERT INTO broken_table_1 SELECT number from numbers(10, 10)")

    assert node.query("SELECT sum(key) FROM broken_table_1") == "190\n"
    assert (
        node.query(
            "SELECT name FROM system.parts where table = 'broken_table_1' and active order by name"
        )
        == "all_0_0_0\nall_1_1_0\n"
    )

    break_part("broken_table_1", "all_0_0_0")

    detach_table("broken_table_1")
    with pytest.raises(QueryRuntimeException):
        attach_table("broken_table_1")


def test_corrupted_unexpected_part():
    node.query(
        """
    CREATE TABLE broken_table_2 (key Int) ENGINE = ReplicatedMergeTree('/tables/broken_2', '1') ORDER BY tuple()
    SETTINGS max_suspicious_broken_parts = 0, replicated_max_ratio_of_wrong_parts=0"""
    )

    node.query("INSERT INTO broken_table_2 SELECT number from numbers(10)")
    node.query("INSERT INTO broken_table_2 SELECT number from numbers(10, 10)")

    node.query("OPTIMIZE TABLE broken_table_2 FINAL")

    assert node.query("SELECT sum(key) FROM broken_table_2") == "190\n"
    assert (
        node.query(
            "SELECT name FROM system.parts where table = 'broken_table_2' and active"
        )
        == "all_0_1_1\n"
    )

    remove_part_from_zookeeper("/tables/broken_2/replicas/1", "all_0_0_0")
    break_part("broken_table_2", "all_0_0_0")

    detach_table("broken_table_2")
    attach_table("broken_table_2")

    assert node.query("SELECT sum(key) FROM broken_table_2") == "190\n"
    assert (
        node.query(
            "SELECT name FROM system.parts where table = 'broken_table_2' and active"
        )
        == "all_0_1_1\n"
    )


def test_corrupted_unexpected_part_ultimate():
    node.query(
        """
    CREATE TABLE broken_table_3 (key Int) ENGINE = ReplicatedMergeTree('/tables/broken_3', '1') ORDER BY tuple()
    SETTINGS max_suspicious_broken_parts = 0, replicated_max_ratio_of_wrong_parts=0"""
    )

    node.query("INSERT INTO broken_table_3 SELECT number from numbers(10)")
    node.query("INSERT INTO broken_table_3 SELECT number from numbers(10, 10)")

    node.query("OPTIMIZE TABLE broken_table_3 FINAL")

    assert node.query("SELECT sum(key) FROM broken_table_3") == "190\n"
    assert (
        node.query(
            "SELECT name FROM system.parts where table = 'broken_table_3' and active"
        )
        == "all_0_1_1\n"
    )

    remove_part_from_zookeeper("/tables/broken_3/replicas/1", "all_0_0_0")
    break_part("broken_table_3", "all_0_0_0")
    remove_part_from_zookeeper("/tables/broken_3/replicas/1", "all_0_1_1")

    detach_table("broken_table_3")
    attach_table("broken_table_3")

    assert (
        node.query(
            "SELECT is_readonly FROM system.replicas WHERE table = 'broken_table_3'"
        )
        == "1\n"
    )

    assert node.query("SELECT sum(key) FROM broken_table_3") == "145\n"
