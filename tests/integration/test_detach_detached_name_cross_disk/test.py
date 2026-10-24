import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/storage_configuration.xml"],
    with_zookeeper=True,
    stay_alive=True,
    tmpfs=["/jbod1:size=60M", "/jbod2:size=60M", "/readonly_local:size=60M"],
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


def data_path_on(table, disk):
    return node.query(
        f"SELECT arrayFilter(x -> position(x, '/{disk}/') > 0, data_paths)[1] "
        f"FROM system.tables WHERE database = currentDatabase() AND name = '{table}'"
    ).strip()


def active_part(table):
    """The part name, read rather than assumed: a plain MergeTree numbers blocks from 1."""
    return node.query(
        f"SELECT name FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{table}' AND active"
    ).strip()


def fill_every_candidate_name(table, disk, part, prefix=""):
    """Occupy '<prefix>_<part>' and its 9 '_tryN' variants on `disk` only.

    The allocator tries exactly 10 names, so this leaves it nothing to pick. Creating the
    directories directly is what test_partition/test.py already does for this allocator.
    """
    detached = data_path_on(table, disk) + "detached"
    base = f"{prefix}_{part}" if prefix else part
    names = [base] + [f"{base}_try{i}" for i in range(1, 10)]
    node.exec_in_container(
        ["bash", "-c", "mkdir -p " + " ".join(f"'{detached}/{n}'" for n in names)],
        privileged=True,
    )
    return names


def test_exhausted_names_on_another_disk_fail_closed(start_cluster):
    """ignore_error = false: a detach that cannot pick a free name must fail loudly.

    Before the table-wide search the 10th attempt returned a name taken on the other disk and
    the caller's own-disk collision guard let it through, creating a duplicate.
    """
    node.query("DROP TABLE IF EXISTS excl SYNC")
    node.query(
        """
        CREATE TABLE excl (k UInt64, v String) ENGINE = MergeTree ORDER BY k
        SETTINGS storage_policy = 'two_disks', old_parts_lifetime = 100000
        """
    )
    node.query("SYSTEM STOP MERGES excl")
    node.query(f"INSERT INTO excl {INSERT_SETTINGS} VALUES (1, 'live')")
    part = active_part("excl")
    assert part != ""

    other = "jbod2" if active_disk("excl") == "jbod1" else "jbod1"
    taken = fill_every_candidate_name("excl", other, part)
    assert (
        node.query(
            "SELECT count() FROM system.detached_parts "
            "WHERE database = currentDatabase() AND table = 'excl'"
        ).strip()
        == "10"
    )

    assert "DIRECTORY_ALREADY_EXISTS" in node.query_and_get_error(
        f"ALTER TABLE excl DETACH PART '{part}'"
    )

    # Failing closed means no eleventh directory was created and the part is still attached.
    assert (
        node.query(
            "SELECT count(), uniqExact(name) FROM system.detached_parts "
            "WHERE database = currentDatabase() AND table = 'excl'"
        ).strip()
        == f"{len(taken)}\t{len(taken)}"
    )
    assert node.query("SELECT count() FROM excl").strip() == "1"


def swallowed_exhaustion_since_last_start(instance):
    """Count name-exhaustion failures swallowed by renameToDetached during the current server run.

    Scoped to the current run and attributed to the swallowing frame, because neither property
    alone discriminates: the sentence is also logged by a user query in the sibling exhaustion
    test, and also by AsyncLoader::worker when the exception escapes instead of being swallowed.

    The window is cut at the last startup banner rather than at a line offset taken before the
    restart: the integration logger runs with rotateOnOpen, so a restart opens a fresh
    clickhouse-server.log and a pre-restart offset would point past the startup region.
    """
    counted = instance.exec_in_container(
        [
            "bash",
            "-c",
            "awk '/Application: Starting ClickHouse/ { n = 0 } "
            "/renameToDetached.*Cannot find a free directory name to detach to/ { n++ } "
            "END { print n + 0 }' /var/log/clickhouse-server/clickhouse-server.log",
        ],
    )
    return int(counted.strip())


def test_exhausted_names_do_not_kill_the_server_on_start(start_cluster):
    """ignore_error = true: a startup detach must log and skip, never abort the process.

    The broken-on-start rename passes ignore_error = true on a Replicated table, and two of its
    callers sit in function-try-blocks that terminate the server, so this exhaustion has to be
    swallowed rather than escalated.
    """
    node.query("DROP TABLE IF EXISTS surv SYNC")
    node.query(
        """
        CREATE TABLE surv (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/surv', 'r1') ORDER BY k
        SETTINGS storage_policy = 'two_disks', old_parts_lifetime = 100000
        """
    )
    node.query("SYSTEM STOP MERGES surv")
    node.query(f"INSERT INTO surv {INSERT_SETTINGS} VALUES (1, 'live')")
    part = active_part("surv")
    assert part != ""

    live = active_disk("surv")
    other = "jbod2" if live == "jbod1" else "jbod1"
    names = fill_every_candidate_name("surv", other, part, prefix="broken-on-start")
    blocked = data_path_on("surv", other) + "detached"

    # Make the part fail to load, so the startup path detaches it as broken-on-start. Corrupting
    # the checksums file is what makes loadDataPart mark the part broken while it is still in the
    # expected set (deleting a file it needs instead drops the part before any detach is tried).
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"echo broken > '{data_path_on('surv', live)}{part}/checksums.txt'",
        ],
        privileged=True,
    )

    try:
        node.restart_clickhouse(kill=True)

        # Without this the assertions below would hold trivially: they only mean something if the
        # startup detach really ran out of names and the exception was swallowed rather than raised.
        # Both halves of the check below are load-bearing. Windowing to this server run excludes
        # the sibling exhaustion test, which logs the same sentence through a user query; and
        # requiring renameToDetached's own logger prefix excludes the exception ESCAPING instead
        # of being swallowed, which is logged by AsyncLoader::worker.
        assert swallowed_exhaustion_since_last_start(node) > 0

        # The whole point: the server is up and answering after swallowing the exhaustion.
        assert node.query("SELECT 1").strip() == "1"
        assert (
            node.query(
                "SELECT count() FROM system.tables "
                "WHERE database = currentDatabase() AND name = 'surv'"
            ).strip()
            == "1"
        )
    finally:
        # A table left unloadable would fail every later system.tables read in this module, so
        # free the names and restart before dropping it whatever the assertions did.
        node.exec_in_container(
            ["bash", "-c", "rm -rf " + " ".join(f"'{blocked}/{n}'" for n in names)],
            privileged=True,
        )
        node.restart_clickhouse(kill=True)
        node.query("DROP TABLE IF EXISTS surv SYNC")


def test_other_exceptions_still_propagate_when_ignoring_errors(start_cluster):
    """ignore_error = true tolerates a name-exhaustion failure only, never a storage failure.

    Name resolution shares its try block with the rename, so an unqualified handler would also
    swallow a rename that failed for an unrelated reason - and the caller erases the part from
    memory right after this returns, which would forget a part still sitting in its old directory.

    Driven through SYSTEM RESTORE REPLICA, which detaches every part via
    forcefullyMovePartToDetachedAndRemoveFromMemory and so passes ignore_error = true on a
    Replicated table. The rewritable-metadata disk is what makes the move raise a DB::Exception
    (a local disk only ever raises fs::filesystem_error there, which a pre-existing handler owns).
    """
    # The injected fault aborts a metadata transaction midway, so neither the table nor its
    # Keeper path is reusable afterwards: give every run its own names and leave them behind.
    table = "prop_" + uuid.uuid4().hex[:8]
    node.query(
        f"""
        CREATE TABLE {table} (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/{table}', 'r1') ORDER BY k
        SETTINGS storage_policy = 'rewritable_only', old_parts_lifetime = 100000
        """
    )
    node.query(f"SYSTEM STOP MERGES {table}")
    node.query(f"INSERT INTO {table} {INSERT_SETTINGS} VALUES (1, 'live')")
    assert node.query(f"SELECT count() FROM {table}").strip() == "1"

    # RESTORE REPLICA requires the replica to be readonly with no metadata in Keeper.
    cluster.get_kazoo_client("zoo1").delete(f"/clickhouse/{table}", recursive=True)
    node.query(f"SYSTEM RESTART REPLICA {table}")
    assert (
        node.query(f"SELECT is_readonly FROM system.replicas WHERE table = '{table}'").strip()
        == "1"
    )

    try:
        node.query("SYSTEM ENABLE FAILPOINT plain_object_storage_write_fail_on_directory_move")
        error = node.query_and_get_error(f"SYSTEM RESTORE REPLICA {table}")

        # FAULT_INJECTED, not DIRECTORY_ALREADY_EXISTS: it must reach the caller even though
        # this detach opted into ignoring errors.
        assert "FAULT_INJECTED" in error, error
    finally:
        node.query("SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_move")
        # Detach rather than drop: the replica is readonly with no metadata in Keeper, so a
        # replicated DROP waits for a state that cannot be reached. PERMANENTLY frees the name.
        node.query(f"DETACH TABLE {table} PERMANENTLY SYNC")


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


def test_detach_ignores_a_name_taken_only_on_a_non_enumerable_disk(start_cluster):
    """A name occupied on a read-only disk must not push the live part to a '_tryN' directory.

    system.detached_parts skips read-only and write-once disks, and ATTACH PARTITION drops every
    '_tryN' candidate. So if the allocator treated such a name as taken, the detached copy would
    land under a name nothing can attach while the occupied one stays invisible, leaving the
    partition unattachable even though both copies are on disk.
    """
    node.query("DROP TABLE IF EXISTS dst_ro SYNC")
    node.query(
        """
        CREATE TABLE dst_ro (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/dst_ro', 'r1') ORDER BY k
        SETTINGS storage_policy = 'writable_and_readonly', old_parts_lifetime = 100000
        """
    )
    node.query("SYSTEM STOP MERGES dst_ro")
    node.query(f"INSERT INTO dst_ro {INSERT_SETTINGS} VALUES (1, 'from_dst')")
    part = active_part("dst_ro")

    # The live part has to be on the writable disk for the scenario to mean anything.
    assert active_disk("dst_ro") == "jbod1"

    # Occupy the same name on the read-only disk of the very same policy.
    detached = data_path_on("dst_ro", "readonly_local") + "detached"
    node.exec_in_container(
        ["bash", "-c", f"mkdir -p '{detached}/{part}'"], privileged=True
    )
    # Precondition: that copy is invisible to the enumeration ATTACH PARTITION resolves against.
    assert (
        node.query(
            "SELECT count() FROM system.detached_parts WHERE database = currentDatabase() "
            "AND table = 'dst_ro'"
        ).strip()
        == "0"
    )

    node.query(f"ALTER TABLE dst_ro DETACH PART '{part}'")

    # The name is free as far as every reader is concerned, so it must be used as is.
    assert (
        node.query(
            "SELECT name FROM system.detached_parts WHERE database = currentDatabase() "
            "AND table = 'dst_ro' ORDER BY name"
        )
        == f"{part}\n"
    )

    node.query("ALTER TABLE dst_ro ATTACH PARTITION tuple()")
    assert node.query("SELECT count() FROM dst_ro").strip() == "1"
    assert node.query("SELECT v FROM dst_ro").strip() == "from_dst"
