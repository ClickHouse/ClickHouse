import os

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True, stay_alive=True)

EXPECTED = "1\t10\tall_0_0_0_1\n1\t20\tall_0_0_0_1\n1\t30\tall_0_0_0_1\n"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def part_path(table, part):
    path = node.query(
        f"SELECT path FROM system.parts WHERE database = currentDatabase()"
        f" AND table = '{table}' AND name = '{part}'"
    ).strip()
    # The path is handed to a root `rm` below, so a wrong or empty answer from
    # `system.parts` must fail loudly here instead of deleting something else.
    assert path.startswith("/var/lib/clickhouse/"), f"unexpected part path: {path}"
    return path


def remove_data_bin(path):
    node.exec_in_container(
        ["rm", "-f", f"{path}/data.bin"], privileged=True, user="root"
    )


def test_broken_parts_chain_on_start(started_cluster):
    node.query("DROP TABLE IF EXISTS rmt1 SYNC")
    node.query("DROP TABLE IF EXISTS rmt2 SYNC")

    for replica in ("r1", "r2"):
        node.query(
            f"CREATE TABLE rmt{replica[1]} (a Int32, b Int32)"
            f" ENGINE = ReplicatedMergeTree('/test/broken_parts_chain_on_start/rmt', '{replica}')"
            f" ORDER BY a SETTINGS old_parts_lifetime = 100500"
        )

    node.query(
        "INSERT INTO rmt1 VALUES (1, 1), (1, 2), (1, 3)",
        settings={"insert_keeper_fault_injection_probability": 0},
    )
    # Mutating b leaves all_0_0_0 Outdated and makes all_0_0_0_1 the active part,
    # so the chain of a covered broken part under a broken part is what gets loaded.
    node.query(
        "ALTER TABLE rmt1 UPDATE b = b * 10 WHERE 1", settings={"mutations_sync": 1}
    )
    node.query("SYSTEM SYNC REPLICA rmt2")
    assert node.query("SELECT a, b, _part FROM rmt2 ORDER BY b") == EXPECTED

    # The part path contains the table UUID, so its parent directory identifies
    # this run's table even though the server (and its log) is shared with other tests.
    table_dir = os.path.dirname(
        part_path("rmt1", "all_0_0_0").rstrip("/").removeprefix("/var/lib/clickhouse/")
    )
    remove_data_bin(part_path("rmt1", "all_0_0_0"))
    remove_data_bin(part_path("rmt1", "all_0_0_0_1"))

    # A kill + restart exercises the on-start part-load path the broken-part
    # detection lives on; DETACH/ATTACH would only re-enter it partially.
    node.restart_clickhouse(kill=True)
    node.query("SYSTEM WAIT LOADING PARTS rmt1")

    detached = node.query(
        "SELECT name FROM system.detached_parts WHERE database = currentDatabase()"
        " AND table = 'rmt1' ORDER BY name"
    )
    assert (
        detached == "broken-on-start_all_0_0_0\nbroken-on-start_all_0_0_0_1\n"
    ), detached

    # Pin the code site: both parts must be rejected while loading, by the part
    # checksum check for the removed data.bin, not by some other consistency check.
    # restart_clickhouse rotates the log, so grep every rotation for this table's UUID.
    missing = node.grep_in_log("data.bin doesn't exist")
    assert len([l for l in missing.splitlines() if table_dir in l]) >= 2, missing
    detaching = node.grep_in_log("Detaching broken part")
    assert len([l for l in detaching.splitlines() if table_dir in l]) >= 2, detaching

    node.query("SYSTEM SYNC REPLICA rmt1")
    assert node.query("SELECT a, b, _part FROM rmt1 ORDER BY b") == EXPECTED
    assert (
        node.query(
            "SELECT count() FROM system.replicas"
            " WHERE database = currentDatabase() AND lost_part_count != 0"
        )
        == "0\n"
    )
