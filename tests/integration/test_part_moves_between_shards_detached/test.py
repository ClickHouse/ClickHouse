import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

s0 = cluster.add_instance(
    "s0",
    main_configs=["configs/merge_tree.xml"],
    with_zookeeper=True,
)
s1 = cluster.add_instance(
    "s1",
    main_configs=["configs/merge_tree.xml", "configs/storage_configuration.xml"],
    with_zookeeper=True,
    tmpfs=["/jbod1:size=100M", "/jbod2:size=100M", "/write_once:size=100M"],
)

PART = "all_0_0_0"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(prefix, dst_settings=""):
    """Two INDEPENDENT tables, so the destination only ever receives the part through the
    MOVE PART TO SHARD under test. Both start empty, so both name their first part all_0_0_0.

    The name carries a fresh suffix: the assertions below read the shared server log, so a repeated
    run must not match the previous one's lines."""
    name = f"{prefix}_{uuid.uuid4().hex[:8]}"
    for node, shard, settings in ((s0, "s0", ""), (s1, "s1", dst_settings)):
        node.query(
            f"""
            DROP TABLE IF EXISTS {name} SYNC;
            CREATE TABLE {name} (k UInt64, v String)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{name}_{shard}', 'r1')
            ORDER BY k SETTINGS old_parts_lifetime = 100000{settings}
        """
        )
        node.query(f"SYSTEM STOP MERGES {name}")
    return name


def drop_tables(name):
    for node in (s0, s1):
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def detached_rows(node, name, columns="name, disk"):
    # Only published parts have a parsed, prefix-less name (reason = ''). A refused clone is
    # retried, and each retry stages and then deletes directories under detached/ whose names
    # carry a prefix or do not parse; those must not count as rows here.
    return node.query(
        f"SELECT {columns} FROM system.detached_parts WHERE database = currentDatabase()"
        f" AND table = '{name}' AND reason = '' ORDER BY name, disk"
    ).strip()


def logged_for_table(node, name, substring):
    """Whether `substring` was logged for THIS table. The log is shared by every test on the
    node, so the table name has to be part of the match."""
    lines = node.grep_in_log(substring)
    return any(f".{name} (" in line for line in lines.splitlines())


def published_to_detached(node, name):
    return logged_for_table(node, name, f"Cloned part {PART} to detached directory")


def reused_detached_part(node, name):
    return logged_for_table(node, name, "is already in the detached directory with the same checksum")


def wait_for_move_state(node, name, state, timeout=120):
    deadline = time.monotonic() + timeout
    while True:
        actual = node.query(
            f"SELECT state FROM system.part_moves_between_shards"
            f" WHERE database = currentDatabase() AND table = '{name}'"
        ).strip()
        if actual == state:
            return
        assert time.monotonic() < deadline, f"move state is {actual!r}, expected {state!r}"
        time.sleep(0.5)


def wait_for_clone_outcome(node, name, timeout=120):
    """Return once the destination has acted on the clone. Refusing is the wanted outcome and is
    reported on its queue entry, which is retried forever; publishing over the occupant and
    mistaking it for our own leftover are the two failure modes, and both are returned here so the
    caller rejects them by name instead of timing out."""
    deadline = time.monotonic() + timeout
    while True:
        exception = node.query(
            f"SELECT last_exception FROM system.replication_queue"
            f" WHERE database = currentDatabase() AND table = '{name}'"
            f" AND type = 'CLONE_PART_FROM_SHARD'"
        ).strip()
        if (
            "DIRECTORY_ALREADY_EXISTS" in exception
            or published_to_detached(node, name)
            or reused_detached_part(node, name)
        ):
            return
        assert time.monotonic() < deadline, f"clone did not run, last exception: {exception!r}"
        time.sleep(0.5)


def move_to_shard(name):
    s0.query(
        f"ALTER TABLE {name} MOVE PART '{PART}' TO SHARD '/clickhouse/tables/{name}_s1'"
    )


def test_move_does_not_destroy_foreign_detached_part(started_cluster):
    """The destination parked its own part under the name the incoming clone wants."""
    name = create_tables("t_conflict")

    s0.query(f"INSERT INTO {name} VALUES (1, 'from_s0')")
    s1.query(f"INSERT INTO {name} VALUES (2, 'parked_on_s1')")
    s1.query(f"ALTER TABLE {name} DETACH PART '{PART}'")
    detached_bytes = s1.query(
        f"SELECT bytes_on_disk FROM system.detached_parts WHERE database = currentDatabase()"
        f" AND table = '{name}' AND name = '{PART}'"
    ).strip()

    move_to_shard(name)
    wait_for_clone_outcome(s1, name)

    assert not published_to_detached(s1, name), "clone published over an occupied name"
    assert not reused_detached_part(s1, name), "a foreign part was mistaken for our own leftover"
    # Still exactly one detached/<part>, same size, and it still holds the parked row.
    assert detached_rows(s1, name, "name") == PART
    assert (
        s1.query(
            f"SELECT bytes_on_disk FROM system.detached_parts WHERE database = currentDatabase()"
            f" AND table = '{name}' AND name = '{PART}'"
        ).strip()
        == detached_bytes
    )
    s1.query(f"ALTER TABLE {name} ATTACH PART '{PART}'")
    assert s1.query(f"SELECT v FROM {name} WHERE k = 2").strip() == "parked_on_s1"

    # The refusal is recoverable: freeing the name lets the retried entry finish the move.
    wait_for_move_state(s0, name, "DONE")
    assert s0.query(f"SELECT count() FROM {name}").strip() == "0"
    assert s1.query(f"SELECT k, v FROM {name} ORDER BY k").strip() == "1\tfrom_s0\n2\tparked_on_s1"

    drop_tables(name)


def test_move_refuses_conflict_on_another_disk(started_cluster):
    """The conflicting directory sits on another disk of the destination's policy.

    `rename`'s own guard only looks at the clone's own disk, so without a table-wide check the
    clone publishes and two logical detached/<part> exist until ATTACH consumes one."""
    name = create_tables("t_disks", dst_settings=", storage_policy = 'two_disks'")

    s0.query(f"INSERT INTO {name} VALUES (1, 'from_s0')")
    s1.query(f"INSERT INTO {name} VALUES (2, 'parked_on_s1')")
    s1.query(f"ALTER TABLE {name} MOVE PART '{PART}' TO DISK 'jbod2'")
    s1.query(f"ALTER TABLE {name} DETACH PART '{PART}'")
    # The clone reserves from the first volume, so the two sides are provably on different disks -
    # otherwise this degenerates into the single-disk case above.
    assert detached_rows(s1, name) == f"{PART}\tjbod2"

    move_to_shard(name)
    wait_for_clone_outcome(s1, name)

    assert not published_to_detached(s1, name), "clone published over an occupied name"
    assert not reused_detached_part(s1, name), "a foreign part was mistaken for our own leftover"
    assert detached_rows(s1, name) == f"{PART}\tjbod2"

    s1.query(f"ALTER TABLE {name} ATTACH PART '{PART}'")
    wait_for_move_state(s0, name, "DONE")
    assert s1.query(f"SELECT k, v FROM {name} ORDER BY k").strip() == "1\tfrom_s0\n2\tparked_on_s1"

    drop_tables(name)


def test_move_reuses_its_own_published_part(started_cluster):
    """A checksum-identical occupant is this entry's own earlier publication, so the retried
    entry must accept it: refusing here would stall the move before DESTINATION_ATTACH.

    FETCH PART leaves exactly what an interrupted attempt of the clone leaves - the same part,
    fetched from the same replica, under its canonical detached name."""
    name = create_tables("t_leftover")

    s0.query(f"INSERT INTO {name} VALUES (1, 'from_s0')")
    s1.query(f"ALTER TABLE {name} FETCH PART '{PART}' FROM '/clickhouse/tables/{name}_s0'")
    assert detached_rows(s1, name, "name") == PART

    move_to_shard(name)
    wait_for_move_state(s0, name, "DONE")

    assert reused_detached_part(s1, name), "the identical occupant was not reused"
    assert not published_to_detached(s1, name), "the clone republished over an identical occupant"
    assert s0.query(f"SELECT count() FROM {name}").strip() == "0"
    assert s1.query(f"SELECT k, v FROM {name} ORDER BY k").strip() == "1\tfrom_s0"

    drop_tables(name)


def test_move_refuses_conflict_on_a_disk_attach_cannot_see(started_cluster):
    """The occupant sits on a write-once disk, which `getDetachedParts` deliberately skips.

    `DESTINATION_ATTACH` builds its candidates from that enumeration, so accepting such an
    occupant as this entry's own publication would report success here and then fail at attach
    time with an unrelated-looking NO_REPLICA_HAS_PART. Reuse has to mean "the publication attach
    will consume", so the clone must refuse here instead."""
    name = create_tables("t_hidden", dst_settings=", storage_policy = 'with_write_once'")

    s0.query(f"INSERT INTO {name} VALUES (1, 'from_s0')")
    # A write-once disk rejects FETCH PART and MOVE PART, which is why such an occupant only ever
    # arrives as a directory - "produced on another machine", as getDetachedParts puts it. The copy
    # is byte-identical to the part the move will fetch, so only the disk makes it unreusable.
    src_part = s0.query(
        f"SELECT path FROM system.parts WHERE database = currentDatabase()"
        f" AND table = '{name}' AND active"
    ).strip().rstrip("/")
    dst_uuid = s1.query(
        f"SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase()"
        f" AND name = '{name}'"
    ).strip()
    detached_dir = f"/write_once/store/{dst_uuid[:3]}/{dst_uuid}/detached"
    occupant = f"{detached_dir}/{PART}"
    started_cluster.copy_file_from_container_to_container(s0, src_part, s1, "/tmp/")
    s1.exec_in_container(
        ["bash", "-c", f"mkdir -p {detached_dir} && cp -r /tmp/{PART} {occupant}"]
    )
    # The premise of the whole test: the probe finds it, the enumeration does not.
    assert detached_rows(s1, name) == "", "the occupant must be invisible to getDetachedParts"

    move_to_shard(name)
    wait_for_clone_outcome(s1, name)

    assert not reused_detached_part(s1, name), "an occupant ATTACH cannot see was reused"
    assert not published_to_detached(s1, name), "clone published over an occupied name"
    assert "DIRECTORY_ALREADY_EXISTS" in s1.query(
        f"SELECT last_exception FROM system.replication_queue"
        f" WHERE database = currentDatabase() AND table = '{name}'"
        f" AND type = 'CLONE_PART_FROM_SHARD'"
    ), "the refusal has to name the conflict on this entry, not stall later at attach"
    # Never modified, never removed.
    assert (
        s1.exec_in_container(["bash", "-c", f"cd {occupant} && md5sum * | sort"])
        == s1.exec_in_container(["bash", "-c", f"cd /tmp/{PART} && md5sum * | sort"])
    )

    drop_tables(name)


def test_move_without_conflict(started_cluster):
    """Control: passes on unpatched master too, so the suite above is specific to the conflict."""
    name = create_tables("t_control")

    s0.query(f"INSERT INTO {name} VALUES (1, 'from_s0')")

    move_to_shard(name)
    wait_for_move_state(s0, name, "DONE")

    assert s0.query(f"SELECT count() FROM {name}").strip() == "0"
    assert s1.query(f"SELECT k, v FROM {name} ORDER BY k").strip() == "1\tfrom_s0"
    assert detached_rows(s1, name, "name") == ""

    drop_tables(name)
