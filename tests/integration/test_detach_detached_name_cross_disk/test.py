import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/storage_configuration.xml"],
    with_zookeeper=True,
    tmpfs=["/jbod1:size=60M", "/jbod2:size=60M"],
)

# A retried insert would allocate a different block number, and the whole scenario relies on the
# two tables producing the same part name.
INSERT_SETTINGS = "SETTINGS insert_keeper_fault_injection_probability = 0"

PART = "all_0_0_0"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(suffix, src_value, dst_value):
    """Two independent tables holding the same part name but different data.

    Only the destination table uses the multi-disk policy: the source table exists just to
    provide a part to fetch.
    """
    src = f"src_{suffix}"
    dst = f"dst_{suffix}"
    for name in (src, dst):
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")

    node.query(
        f"""
        CREATE TABLE {src} (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/{src}', 'r1')
        ORDER BY k
        SETTINGS old_parts_lifetime = 100000
        """
    )
    node.query(
        f"""
        CREATE TABLE {dst} (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/{dst}', 'r1')
        ORDER BY k
        SETTINGS storage_policy = 'two_disks', old_parts_lifetime = 100000
        """
    )

    # Merges would rename the parts out from under the assertions below.
    node.query(f"SYSTEM STOP MERGES {src}")
    node.query(f"SYSTEM STOP MERGES {dst}")

    node.query(f"INSERT INTO {src} {INSERT_SETTINGS} VALUES (1, '{src_value}')")
    node.query(f"INSERT INTO {dst} {INSERT_SETTINGS} VALUES (1, '{dst_value}')")

    for name in (src, dst):
        assert (
            node.query(
                f"SELECT name FROM system.parts WHERE database = currentDatabase() "
                f"AND table = '{name}' AND active"
            ).strip()
            == PART
        )

    return src, dst


def detached_disk(table):
    return node.query(
        f"SELECT disk FROM system.detached_parts WHERE database = currentDatabase() "
        f"AND table = '{table}' ORDER BY name LIMIT 1"
    ).strip()


def active_disk(table):
    return node.query(
        f"SELECT disk_name FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{table}' AND active"
    ).strip()


def split_across_disks(src, dst):
    """FETCH publishes detached/<PART>, then MOVE PART puts the live part on the other disk.

    This is the only way to get a detached directory and its live namesake onto different disks:
    a DETACH covers the active part, and ATTACH re-activates the copy on the disk it already
    sits on, so a table can never split them using its own detaches.
    """
    node.query(f"ALTER TABLE {dst} FETCH PART '{PART}' FROM '/clickhouse/{src}'")
    det = detached_disk(dst)
    live = active_disk(dst)
    if live == det:
        other = "jbod2" if live == "jbod1" else "jbod1"
        node.query(f"ALTER TABLE {dst} MOVE PART '{PART}' TO DISK '{other}'")

    # The repro is only valid if the two really ended up on different disks.
    assert active_disk(dst) != detached_disk(dst)


def test_detach_does_not_reuse_a_detached_name_from_another_disk(start_cluster):
    src, dst = create_tables("names", "from_src", "from_dst")
    split_across_disks(src, dst)

    node.query(f"ALTER TABLE {dst} DETACH PART '{PART}'")

    # detached/ is resolved table-wide, so the second directory must not reuse the name.
    assert (
        node.query(
            f"SELECT count(), uniqExact(name) FROM system.detached_parts "
            f"WHERE database = currentDatabase() AND table = '{dst}'"
        ).strip()
        == "2\t2"
    )
    assert node.query(
        f"SELECT name FROM system.detached_parts WHERE database = currentDatabase() "
        f"AND table = '{dst}' ORDER BY name"
    ) == f"{PART}\n{PART}_try1\n"

    # A "_tryN" copy is a leftover, so it must not be a candidate for ATTACH PARTITION.
    node.query(f"ALTER TABLE {dst} ATTACH PARTITION tuple()")
    assert node.query(f"SELECT count() FROM {dst}").strip() == "1"
    assert node.query(f"SELECT v FROM {dst}").strip() == "from_src"
    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE database = currentDatabase() "
            f"AND table = '{dst}' AND active"
        ).strip()
        == "1"
    )


def test_attach_part_after_cross_disk_detach_is_deterministic(start_cluster):
    src, dst = create_tables("attach", "from_src", "from_dst")
    split_across_disks(src, dst)

    node.query(f"ALTER TABLE {dst} DETACH PART '{PART}'")

    # Exactly one detached directory carries a parsable part name, so ATTACH PART is unambiguous.
    node.query(f"ALTER TABLE {dst} ATTACH PART '{PART}'")
    assert node.query(f"SELECT count() FROM {dst}").strip() == "1"
    assert node.query(f"SELECT v FROM {dst}").strip() == "from_src"

    # The leftover copy stays droppable.
    node.query(
        f"ALTER TABLE {dst} DROP DETACHED PART '{PART}_try1' SETTINGS allow_drop_detached = 1"
    )
    assert (
        node.query(
            f"SELECT count() FROM system.detached_parts WHERE database = currentDatabase() "
            f"AND table = '{dst}'"
        ).strip()
        == "0"
    )


def test_single_disk_policy_is_unaffected(start_cluster):
    """Control: with one disk the table-wide predicate must answer like the old own-disk probe."""
    for name in ("src_one", "dst_one"):
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(
        """
        CREATE TABLE src_one (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/src_one', 'r1') ORDER BY k
        SETTINGS old_parts_lifetime = 100000
        """
    )
    node.query(
        """
        CREATE TABLE dst_one (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/dst_one', 'r1') ORDER BY k
        SETTINGS old_parts_lifetime = 100000
        """
    )
    node.query("SYSTEM STOP MERGES src_one")
    node.query("SYSTEM STOP MERGES dst_one")
    node.query(f"INSERT INTO src_one {INSERT_SETTINGS} VALUES (1, 'from_src')")
    node.query(f"INSERT INTO dst_one {INSERT_SETTINGS} VALUES (1, 'from_dst')")

    node.query(f"ALTER TABLE dst_one FETCH PART '{PART}' FROM '/clickhouse/src_one'")
    assert active_disk("dst_one") == detached_disk("dst_one")

    node.query(f"ALTER TABLE dst_one DETACH PART '{PART}'")
    assert node.query(
        "SELECT name FROM system.detached_parts WHERE database = currentDatabase() "
        "AND table = 'dst_one' ORDER BY name"
    ) == f"{PART}\n{PART}_try1\n"

    node.query("ALTER TABLE dst_one ATTACH PARTITION tuple()")
    assert node.query("SELECT count() FROM dst_one").strip() == "1"
    assert node.query("SELECT v FROM dst_one").strip() == "from_src"
