# A replicated `ATTACH_PART` log entry claims a candidate directory in `detached/` by renaming it
# to `attaching_<dir>`. When the claimed part turns out to be broken it must be renamed aside
# under a `broken_` prefix, which takes it out of ATTACH candidacy and leaves it removable with
# SQL. The fixture removes `count.txt` from a detached part directory, so it manipulates the
# server's on-disk data and lives here rather than in a stateless test.

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def exec_root(node, cmd):
    return node.exec_in_container(["bash", "-c", cmd], privileged=True, user="root")


def entry_exists(node, path):
    return exec_root(node, f"if ls {path} >/dev/null 2>&1; then echo 1; else echo 0; fi").strip()


def detached_parts(node, table, columns="name, reason, partition_id"):
    return node.query(
        f"SELECT {columns} FROM system.detached_parts"
        f" WHERE database = 'default' AND table = '{table}' ORDER BY name"
    )


@pytest.mark.parametrize("name_taken", [False, True], ids=["free_name", "name_taken"])
def test_broken_detached_part_is_quarantined(started_cluster, name_taken):
    table = "t_attach_broken_taken" if name_taken else "t_attach_broken_free"

    for replica, node in enumerate([node1, node2], start=1):
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node.query(
            f"""
            CREATE TABLE {table} (a UInt8)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica}')
            ORDER BY a
            """
        )

    node1.query(f"INSERT INTO {table} VALUES (1)")
    node2.query(f"SYSTEM SYNC REPLICA {table}")

    # The first block number is not necessarily 0 (it comes from a Keeper counter), so read the
    # part name rather than hardcoding it.
    part = node1.query(
        f"SELECT name FROM system.parts"
        f" WHERE database = 'default' AND table = '{table}' AND active"
    ).strip()

    node1.query(f"ALTER TABLE {table} DETACH PARTITION ID 'all' SETTINGS alter_sync = 2")
    assert detached_parts(node1, table, "name") == f"{part}\n"
    assert detached_parts(node2, table, "name") == f"{part}\n"

    detached_dir = node1.query(
        f"SELECT data_paths[1] FROM system.tables WHERE database = 'default' AND name = '{table}'"
    ).strip() + "detached"

    # Break node1's copy only. `checksums.txt` is left intact so the candidate filter still
    # matches the entry's checksum, while loading the part throws the non-retryable
    # `NO_FILE_IN_DATA_PART`.
    exec_root(node1, f"rm -f {detached_dir}/{part}/count.txt")

    # In the `name_taken` arm an earlier quarantine already holds the first candidate name. It
    # must not be overwritten, and the broken part must land on the next `_tryN` name.
    expected_dir = f"broken_{part}_try1" if name_taken else f"broken_{part}"
    if name_taken:
        exec_root(node1, f"mkdir -p {detached_dir}/broken_{part} && touch {detached_dir}/broken_{part}/older")

    # node2 attaches its own intact copy, writing the `ATTACH_PART` log entry that node1 then
    # executes against its own broken copy.
    node2.query(f"ALTER TABLE {table} ATTACH PARTITION ID 'all'")
    node1.query(f"SYSTEM SYNC REPLICA {table}")

    assert node1.query(f"SELECT a FROM {table}") == "1\n"

    # `partition_id` is NULL in `system.detached_parts` for a directory name that does not parse
    # as a detached part name, so asserting it also pins the shape of the name produced here.
    expected_rows = f"{expected_dir}\tbroken\tall\n"
    if name_taken:
        expected_rows = f"broken_{part}\tbroken\tall\n" + expected_rows
    assert detached_parts(node1, table) == expected_rows

    assert entry_exists(node1, f"{detached_dir}/{expected_dir}/checksums.txt") == "1"
    if name_taken:
        assert entry_exists(node1, f"{detached_dir}/broken_{part}/older") == "1"

    assert not node1.contains_in_log("fail to rename part")
    assert not node1.contains_in_log("broken_detached/")

    # The quarantined part has left ATTACH candidacy, so ATTACH no longer fails on it, ...
    node1.query(f"ALTER TABLE {table} ATTACH PARTITION ID 'all'")
    # ... and it can be removed with SQL.
    node1.query(
        f"ALTER TABLE {table} DROP DETACHED PARTITION ID 'all' SETTINGS allow_drop_detached = 1"
    )
    assert detached_parts(node1, table) == ""

    for node in [node1, node2]:
        node.query(f"DROP TABLE {table} SYNC")
