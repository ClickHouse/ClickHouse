import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def corrupt_data_bin_keeping_size(node, table, part_name):
    """Simulate the aftermath of an unclean reboot with delayed allocation:
    file metadata (including the size) is preserved, but the content is zeroed.
    Such a part passes all load-time checks (only file presence and sizes are
    compared against checksums.txt), but any merge or read of it fails with
    'Unknown codec family code: 0' (UNKNOWN_CODEC)."""
    part_path = node.query(
        f"SELECT path FROM system.parts WHERE table = '{table}' AND name = '{part_name}' AND active"
    ).strip()
    assert part_path
    node.exec_in_container(
        [
            "bash",
            "-c",
            f'F="{part_path}data.bin" && dd if=/dev/zero of="$F" bs=$(stat -c%s "$F") count=1 conv=notrunc',
        ],
        privileged=True,
    )


def create_and_corrupt_table(table, create_settings):
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(f"""
        CREATE TABLE {table} (key UInt64, value String)
        ENGINE = MergeTree ORDER BY key
        SETTINGS {create_settings}
        """)

    node.query(f"SYSTEM STOP MERGES {table}")
    for i in range(3):
        node.query(
            f"INSERT INTO {table} SELECT number, randomPrintableASCII(50) FROM numbers({i * 1000}, 1000)"
        )

    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND active"
        )
        == "3\n"
    )

    # Corrupt a "border part" (the one with the max block number), otherwise the merge
    # of the remaining parts would produce a part with the same min/max block numbers.
    corrupt_data_bin_keeping_size(node, table, "all_3_3_0")

    # The part is still active and simple queries that don't touch data.bin work.
    assert node.query(f"SELECT count() FROM {table}") == "3000\n"

    node.query(f"SYSTEM START MERGES {table}")

    # The merge fails with a data corruption error.
    error = node.query_and_get_error(f"OPTIMIZE TABLE {table} FINAL")
    assert "UNKNOWN_CODEC" in error or "Unknown codec family code" in error


def test_detach_broken_part_after_failed_merge(started_cluster):
    table = "test_broken_detach"
    create_and_corrupt_table(table, "detach_broken_parts_after_failed_merge = 1")

    # The background check confirms the corruption and detaches the part.
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.detached_parts WHERE table = '{table}' AND startsWith(name, 'broken_')",
        "1",
    )
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND name = 'all_3_3_0'",
        "0",
    )

    # Merges in the partition are unblocked now.
    node.query(f"OPTIMIZE TABLE {table} FINAL")
    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND active"
        )
        == "1\n"
    )
    assert node.query(f"SELECT count() FROM {table}") == "2000\n"
    assert node.query(f"SELECT sum(key) FROM {table}") == f"{sum(range(2000))}\n"

    assert (
        int(
            node.query(
                "SELECT value FROM system.events WHERE event = 'BrokenPartsDetached'"
            )
        )
        >= 1
    )

    node.query(f"DROP TABLE {table} SYNC")


def test_detach_broken_part_disabled_by_default(started_cluster):
    table = "test_broken_no_detach"
    create_and_corrupt_table(table, "index_granularity = 8192")

    # The setting is disabled by default: the broken part must stay active
    # and must not be detached.
    time.sleep(5)
    assert (
        node.query(f"SELECT count() FROM system.detached_parts WHERE table = '{table}'")
        == "0\n"
    )
    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND name = 'all_3_3_0'"
        )
        == "1\n"
    )

    # After enabling the setting the next failed merge triggers the check and the detach.
    node.query(
        f"ALTER TABLE {table} MODIFY SETTING detach_broken_parts_after_failed_merge = 1"
    )
    error = node.query_and_get_error(f"OPTIMIZE TABLE {table} FINAL")
    assert "UNKNOWN_CODEC" in error or "Unknown codec family code" in error

    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.detached_parts WHERE table = '{table}' AND startsWith(name, 'broken_')",
        "1",
    )

    node.query(f"OPTIMIZE TABLE {table} FINAL")
    assert node.query(f"SELECT count() FROM {table}") == "2000\n"

    node.query(f"DROP TABLE {table} SYNC")


def test_detach_broken_part_respects_limit(started_cluster):
    table = "test_broken_detach_limit"
    create_and_corrupt_table(
        table,
        "detach_broken_parts_after_failed_merge = 1, max_suspicious_broken_parts = 0",
    )

    # The limit on automatically detached parts is zero: the part must not be detached.
    time.sleep(5)
    assert (
        node.query(f"SELECT count() FROM system.detached_parts WHERE table = '{table}'")
        == "0\n"
    )
    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND name = 'all_3_3_0'"
        )
        == "1\n"
    )

    node.query(f"DROP TABLE {table} SYNC")
