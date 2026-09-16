"""A broken part in `detached/` must be renamed out of the way by the `ATTACH_PART` executor.

The executor builds its candidate part with a `detached/attaching_<name>` directory so it can read
the candidate out of `detached/`. Prefixing that whole path aimed the quarantine rename at
`detached/broken_detached/attaching_<name>`, a directory nothing creates, so the rename failed and the
torn part kept its valid name - failing every later attach the same way.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True, stay_alive=True)
node2 = cluster.add_instance("node2", with_zookeeper=True, stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_broken_detached_part_is_renamed_with_broken_prefix(started_cluster):
    for replica, node in [("r1", node1), ("r2", node2)]:
        node.query(
            "CREATE TABLE t (id UInt64, a UInt32, b UInt32, c UInt32) "
            f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_broken_detached', '{replica}') ORDER BY id "
            "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
        )

    node1.query("INSERT INTO t SELECT number, number, number, number FROM numbers(1000)")
    node2.query("SYSTEM SYNC REPLICA t")
    assert node2.query("SELECT count() FROM t") == "1000\n"

    node2.stop_clickhouse()

    # The on-disk state a kill during `DETACH PARTITION` leaves behind: a hard-linked clone of the part
    # that is missing data files, with `checksums.txt` kept so the log entry's checksum matches. Data
    # files are what is removed here: a missing `columns.txt` makes the load raise a `LOGICAL_ERROR`
    # instead, which the sanitizer builds turn into an abort.
    part_path = node2.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/store -type d -name all_0_0_0 | head -1"],
        user="root",
    ).strip()
    assert part_path, "the part directory was not found"
    node2.exec_in_container(
        [
            "bash",
            "-c",
            f"cd {part_path}/.. && mkdir -p detached && cp -al all_0_0_0 detached/all_0_0_0 && "
            "rm detached/all_0_0_0/b.* detached/all_0_0_0/c.*",
        ],
        user="root",
    )

    node1.query("SYSTEM DROP REPLICA 'r2' FROM TABLE t")
    node2.start_clickhouse()
    node2.query("SYSTEM RESTORE REPLICA t")
    node2.query("SYSTEM SYNC REPLICA t")

    # The rows come back either way (the entry falls back to fetching from `r1`), but the torn part must
    # not keep its valid name: the next attach would parse it again and fail again.
    assert node2.query("SELECT count() FROM t") == "1000\n"

    detached = node2.query(
        "SELECT name FROM system.detached_parts WHERE database = currentDatabase() AND table = 't' ORDER BY name"
    ).split()
    assert "all_0_0_0" not in detached, detached

    # The quarantined directory has to be a plain `broken_<part>`, not `broken_attaching_<part>`: the
    # temporary `attaching_` marker makes the name unparsable, and then `reason` and `partition_id` are
    # `NULL` here and the partition-scoped `DROP DETACHED` below cannot see the part at all.
    quarantined = node2.query(
        "SELECT name, reason, partition_id FROM system.detached_parts "
        "WHERE database = currentDatabase() AND table = 't' AND startsWith(name, 'broken') ORDER BY name"
    )
    assert quarantined == "broken_all_0_0_0\tbroken\tall\n", quarantined

    node2.query("ALTER TABLE t DROP DETACHED PARTITION ALL SETTINGS allow_drop_detached = 1")
    assert (
        node2.query(
            "SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't'"
        )
        == "0\n"
    )

    for node in [node1, node2]:
        node.query("DROP TABLE t SYNC")
