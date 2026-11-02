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
    data_path = node.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='{table}'"
    ).strip()
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mv {data_path}/{part_name}/columns.txt {data_path}/{part_name}/columns.txt.broken",
        ]
    )

def correct_part(data_path, part_name):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mv {data_path}/{part_name}/columns.txt.broken {data_path}/{part_name}/columns.txt",
        ]
    )


def remove_part(table, part_name):
    data_path = node.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='{table}'"
    ).strip()
    node.exec_in_container(["bash", "-c", f"rm -r {data_path}/{part_name}"])


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
    node.query("DROP TABLE IF EXISTS broken_table SYNC")
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

    node.query("DROP TABLE broken_table SYNC")


def test_unexpected_uncommitted_mutation():
    node.query("DROP TABLE IF EXISTS broken_table0 SYNC")
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

    node.query("DROP TABLE broken_table0 SYNC")


def test_corrupted_random_part():
    node.query("DROP TABLE IF EXISTS broken_table_1 SYNC")
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

    data_path = node.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='broken_table_1'"
    ).strip()
    break_part("broken_table_1", "all_0_0_0")

    detach_table("broken_table_1")
    with pytest.raises(QueryRuntimeException):
        attach_table("broken_table_1")

    correct_part(data_path, "all_0_0_0")
    attach_table("broken_table_1")

    node.query("DROP TABLE broken_table_1 SYNC")

def test_corrupted_unexpected_part():
    node.query("DROP TABLE IF EXISTS broken_table_2 SYNC")
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

    node.query("DROP TABLE broken_table_2 SYNC")


def test_corrupted_unexpected_part_ultimate():
    node.query("DROP TABLE IF EXISTS broken_table_3 SYNC")
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

    node.query("DROP TABLE broken_table_3 SYNC")


def bash(cmd):
    return node.exec_in_container(["bash", "-c", cmd], privileged=True, user="root")


def write_first_byte(path, byte_hex):
    bash(f"printf '\\x{byte_hex}' | dd of={path} bs=1 count=1 conv=notrunc")


def test_patch_part_in_unsupported_format_version_is_kept():
    table = "t_unsupported_patch"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
    CREATE TABLE {table} (id UInt64, v Int64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}', 'r1')
    ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
    """
    )
    node.query(f"INSERT INTO {table} SELECT number, 0 FROM numbers(10)")
    node.query(f"UPDATE {table} SET v = 1 WHERE id = 3")
    assert node.query(f"SELECT sum(v) FROM {table}") == "1\n"

    rows = (
        node.query(
            f"SELECT name, path FROM system.parts WHERE database = 'default' AND table = '{table}' AND active AND startsWith(partition_id, 'patch-')"
        )
        .strip()
        .splitlines()
    )
    assert len(rows) == 1, rows
    patch_name, patch_path = rows[0].split("\t")
    assert patch_path.startswith("/"), patch_path
    # The patch part must be an expected part (in Keeper), not an unexpected one.
    assert (
        node.query(
            f"SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/{table}/replicas/r1/parts' AND name = '{patch_name}'"
        )
        == "1\n"
    )

    index_file = f"{patch_path}source_parts.dat"
    original_byte = bash(f"od -An -tx1 -N1 {index_file}").strip()
    size = bash(f"stat -c%s {index_file}").strip()

    detach_table(table)
    write_first_byte(index_file, "7f")
    assert bash(f"stat -c%s {index_file}").strip() == size

    error = node.query_and_get_error(f"ATTACH TABLE {table}")
    assert "UNKNOWN_FORMAT_VERSION" in error, error
    assert patch_name in error, error

    table_path = patch_path.rstrip("/").rsplit("/", 1)[0]
    assert bash(f"test -d {patch_path} && echo yes || echo no").strip() == "yes"
    assert "broken-on-start" not in bash(
        f"ls -A {table_path}/detached/ 2>/dev/null || true"
    )

    write_first_byte(index_file, original_byte)
    attach_table(table)
    assert node.query(f"SELECT sum(v) FROM {table}") == "1\n"
    node.query(f"DROP TABLE {table} SYNC")
