# A merged part of a plain MergeTree has level > 0 while still holding duplicate ORDER BY keys.
# Staged into the detached/ directory of a Replacing/Summing/AggregatingMergeTree and attached, it
# used to keep a non-zero level, and FINAL treats a lone level > 0 part as already collapsed, so
# the duplicates survived SELECT ... FINAL. Regression test for
# https://github.com/ClickHouse/ClickHouse/issues/109674.
#
# No supported statement can put a part of a foreign table into detached/, so the staging step
# writes the server's on-disk data and cannot live in tests/queries/0_stateless. This test owns
# its server and disk layout, next to the sibling conversions in test_attach_tampered_detached_parts.

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# ZooKeeper is required by the ReplicatedReplacingMergeTree case, which adopts the part through
# ReplicatedMergeTreeSink::writeExistingPart rather than StorageMergeTree.
node = cluster.add_instance("node", with_zookeeper=True)

# Two inserts merged into one part: a plain MergeTree merge raises the level and keeps every row.
DUPLICATE_KEY_INSERTS = ["(1, 10), (2, 30), (3, 50)", "(1, 20), (2, 40), (3, 60)"]
KEY_ONLY_INSERTS = ["(1), (2), (3)", "(1), (2), (3)"]
SOURCE_ROWS = 6
DISTINCT_KEYS = 3


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def exec_in_container(cmd):
    # No `user=` override: the server runs as the uid that invoked the test, and ATTACH sets the
    # modification time of the staged directory, which fails with EPERM unless it owns it.
    return node.exec_in_container(["bash", "-c", cmd])


def one_row(query):
    result = node.query(query).strip()
    assert "\n" not in result, f"expected one row from `{query}`, got: {result}"
    return result


def table_data_path(table):
    # length(data_paths) == 1 is asserted rather than assumed: element 1 of a multi-volume
    # storage policy need not be the volume holding the parts.
    assert (
        one_row(
            f"SELECT length(data_paths) FROM system.tables"
            f" WHERE database = 'default' AND table = '{table}'"
        )
        == "1"
    )
    path = one_row(
        f"SELECT data_paths[1] FROM system.tables"
        f" WHERE database = 'default' AND table = '{table}'"
    )
    return path.rstrip("/") + "/"


def active_part(table, column="name"):
    return one_row(
        f"SELECT {column} FROM system.parts"
        f" WHERE database = 'default' AND table = '{table}' AND active"
    )


@pytest.mark.parametrize(
    "engine, extra_columns, inserts",
    [
        ("ReplacingMergeTree", ", b UInt32", DUPLICATE_KEY_INSERTS),
        ("SummingMergeTree", ", b UInt32", DUPLICATE_KEY_INSERTS),
        ("AggregatingMergeTree", "", KEY_ONLY_INSERTS),
        (
            "ReplicatedReplacingMergeTree('/clickhouse/tables/attach_foreign_part_level_reset', 'r1')",
            ", b UInt32",
            DUPLICATE_KEY_INSERTS,
        ),
    ],
    ids=["replacing", "summing", "aggregating", "replicated_replacing"],
)
def test_attach_foreign_part_resets_level(
    started_cluster, engine, extra_columns, inserts
):
    node.query("DROP TABLE IF EXISTS src SYNC")
    node.query("DROP TABLE IF EXISTS dst SYNC")

    node.query(
        f"CREATE TABLE src (a UInt32{extra_columns}) ENGINE = MergeTree ORDER BY a"
    )
    for values in inserts:
        node.query(f"INSERT INTO src VALUES {values}")
    node.query("OPTIMIZE TABLE src FINAL")

    part = active_part("src")
    assert int(active_part("src", "level")) > 0, f"{part} is not a merged part"
    assert one_row("SELECT count() FROM src") == str(SOURCE_ROWS)

    node.query(
        f"CREATE TABLE dst (a UInt32{extra_columns}) ENGINE = {engine} ORDER BY a"
    )
    # A background merge would collapse the lone attached part on its own, and the FINAL
    # assertion below would then hold whatever level the part was attached with.
    node.query("SYSTEM STOP MERGES dst")

    dst_path = table_data_path("dst")
    # Hard links, so the staged copy shares the source part's inodes instead of its bytes.
    exec_in_container(
        f"mkdir -p {dst_path}detached"
        f" && cp -rl {active_part('src', 'path').rstrip('/')} {dst_path}detached/{part}"
    )

    node.query(f"ALTER TABLE dst ATTACH PART '{part}'")

    assert active_part("dst", "level") == "0"
    assert one_row("SELECT count() FROM dst FINAL") == str(DISTINCT_KEYS)
    # Attaching the hard-linked copy must not have rewritten the still-live source part. CHECK TABLE
    # reads src's files off disk and compares them with its recorded checksums. hash_of_all_files is
    # computed from the in-memory checksums, which are not reloaded here, so it cannot observe this.
    assert active_part("src") == part
    assert (
        one_row("CHECK TABLE src SETTINGS check_query_single_value_result = 1") == "1"
    )

    # A level field of 4294967295 is the legacy spelling of MAX_LEVEL: it parses to MAX_LEVEL and the
    # spelling is remembered so that the name round-trips. Forcing the level therefore has to clear
    # that too, or the name is written back with the legacy value and parses as collapsed again.
    prefix, level_field = part.rsplit("_", 1)
    assert part.count("_") == 3 and level_field.isdigit(), part
    legacy_part = f"{prefix}_4294967295"
    exec_in_container(
        f"cp -rl {active_part('src', 'path').rstrip('/')} {dst_path}detached/{legacy_part}"
    )
    node.query(f"ALTER TABLE dst ATTACH PART '{legacy_part}'")
    assert (
        one_row(
            "SELECT count() FROM system.parts WHERE database = 'default' AND table = 'dst'"
            " AND active AND (level != 0 OR name LIKE '%4294967295')"
        )
        == "0"
    )
    assert one_row("SELECT count() FROM dst FINAL") == str(DISTINCT_KEYS)

    node.query("SYSTEM START MERGES dst")
    node.query("OPTIMIZE TABLE dst FINAL")
    assert one_row("SELECT count() FROM dst") == str(DISTINCT_KEYS)

    node.query("DROP TABLE src SYNC")
    node.query("DROP TABLE dst SYNC")
