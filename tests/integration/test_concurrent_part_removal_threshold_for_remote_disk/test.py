"""
Integration test for the MergeTree setting
`concurrent_part_removal_threshold_for_remote_disk`.

The setting is exercised by `MergeTreeData::clearPartsFromFilesystemImplMaybeInParallel`
in `src/Storages/MergeTree/MergeTreeData.cpp`. When any part being removed is on a
remote disk (`IMergeTreeDataPart::isStoredOnRemoteDisk`), the new setting is used
instead of `concurrent_part_removal_threshold` to decide between the serial and the
parallel removal path. Each removal path emits a distinct LOG_DEBUG line:

    Removing N parts from filesystem (serially):    Parts: [...]
    Removing N parts from filesystem (concurrently): Parts: [...]

This test creates fresh MergeTree tables on different storage policies, inserts a
known number of parts, drops the table, and asserts which log line was emitted.

Three scenarios:

1. Parts on remote disk + remote threshold < parts count
       => parallel path expected (verifies the new setting takes effect)

2. Parts on remote disk + remote threshold > parts count
       => serial path expected, even though the legacy
          `concurrent_part_removal_threshold` would have triggered parallel
          (verifies the new setting overrides the legacy one for remote parts)

3. Parts on local disk + remote threshold < parts count, legacy threshold > parts
       => serial path expected
          (verifies the new setting does NOT affect local-disk behavior)

A unique number of parts is used per scenario so that log lines from previous
scenarios cannot accidentally match a later assertion.
"""

import pytest

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    cluster = ClickHouseCluster(__file__)
    cluster.add_instance(
        "node",
        main_configs=["configs/config.d/storage_conf.xml"],
        with_minio=True,
    )
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _create_insert_drop(
    node,
    table_name,
    storage_policy,
    local_threshold,
    remote_threshold,
    num_parts,
):
    """Create a fresh MergeTree, insert `num_parts` distinct parts, DROP it, and return
    a tuple (serial_log, parallel_log) of the strings produced by `grep_in_log` for
    the matching `Removing N parts from filesystem (...)` line.

    Each scenario uses a unique `num_parts` so that the grep cannot accidentally
    match a log line from a different scenario in the same test run.
    """
    node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")

    create_query = f"""
        CREATE TABLE {table_name} (k Int64, v String)
        ENGINE = MergeTree
        PARTITION BY k
        ORDER BY k
        SETTINGS
            storage_policy = '{storage_policy}',
            concurrent_part_removal_threshold = {local_threshold},
            concurrent_part_removal_threshold_for_remote_disk = {remote_threshold}
    """
    node.query(create_query)

    # Disable merges so each INSERT remains a distinct part.
    node.query(f"SYSTEM STOP MERGES {table_name}")

    for i in range(num_parts):
        node.query(f"INSERT INTO {table_name} VALUES ({i}, 'x')")

    active_parts = int(
        node.query(
            f"SELECT count() FROM system.parts "
            f"WHERE database = 'default' AND table = '{table_name}' AND active"
        ).strip()
    )
    assert active_parts == num_parts, (
        f"Expected {num_parts} active parts before DROP, got {active_parts}"
    )

    node.query(f"DROP TABLE {table_name} SYNC")

    # The log lines are scoped by the unique `num_parts` (the `N` in
    # "Removing N parts from filesystem ..."). Parens are literal in basic regex
    # used by zgrep, so the substring matches exactly.
    serial_substr = f"Removing {num_parts} parts from filesystem (serially):"
    parallel_substr = f"Removing {num_parts} parts from filesystem (concurrently):"

    serial_log = node.grep_in_log(serial_substr)
    parallel_log = node.grep_in_log(parallel_substr)

    return serial_log, parallel_log


def test_remote_disk_threshold_triggers_parallel(cluster):
    """Parts on remote disk, remote threshold (1) < parts (5) => parallel removal.

    The legacy `concurrent_part_removal_threshold` is set to 100 (which would have
    forced serial removal on a remote disk before this change). The new setting
    must override it for remote parts.
    """
    node = cluster.instances["node"]
    serial, parallel = _create_insert_drop(
        node,
        table_name="t_remote_parallel",
        storage_policy="s3_only",
        local_threshold=100,
        remote_threshold=1,
        num_parts=5,
    )
    assert parallel, (
        "Expected the parallel removal path on remote disk when "
        "concurrent_part_removal_threshold_for_remote_disk (1) is less than the "
        "number of parts (5). No 'Removing 5 parts from filesystem (concurrently)' "
        "line was found in the server log."
    )
    assert not serial, (
        "Did not expect the serial removal path: "
        f"unexpected log line found: {serial!r}"
    )


def test_remote_disk_threshold_keeps_serial(cluster):
    """Parts on remote disk, remote threshold (100) > parts (7) => serial removal.

    The legacy `concurrent_part_removal_threshold` is set to 1, which on its own
    would have forced parallel removal. The new setting must take precedence for
    remote parts and keep removal serial.
    """
    node = cluster.instances["node"]
    serial, parallel = _create_insert_drop(
        node,
        table_name="t_remote_serial",
        storage_policy="s3_only",
        local_threshold=1,
        remote_threshold=100,
        num_parts=7,
    )
    assert serial, (
        "Expected the serial removal path on remote disk when "
        "concurrent_part_removal_threshold_for_remote_disk (100) is greater than "
        "the number of parts (7). The legacy concurrent_part_removal_threshold (1) "
        "must not be consulted for remote parts. No 'Removing 7 parts from "
        "filesystem (serially)' line was found in the server log."
    )
    assert not parallel, (
        "Did not expect the parallel removal path: "
        f"unexpected log line found: {parallel!r}"
    )


def test_local_disk_unaffected(cluster):
    """Parts on local disk, legacy threshold (100) > parts (11) => serial removal.

    The new setting `concurrent_part_removal_threshold_for_remote_disk` is set to 1
    but must be ignored for local-disk parts. Removal must be governed solely by
    the legacy `concurrent_part_removal_threshold`.
    """
    node = cluster.instances["node"]
    serial, parallel = _create_insert_drop(
        node,
        table_name="t_local_serial",
        storage_policy="local_only",
        local_threshold=100,
        remote_threshold=1,
        num_parts=11,
    )
    assert serial, (
        "Expected the serial removal path on local disk: the legacy "
        "concurrent_part_removal_threshold (100) is greater than the number of "
        "parts (11), and the remote-disk-only setting must not be consulted. "
        "No 'Removing 11 parts from filesystem (serially)' line was found."
    )
    assert not parallel, (
        "Did not expect the parallel removal path: the remote-disk threshold "
        f"must not affect local-disk parts. Unexpected log line: {parallel!r}"
    )
