import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# ZooKeeper is required: classifying a part whose `txn_version.txt` carries a transactional TID
# reaches `TransactionLog::instance()`, whose constructor calls `loadLogFromZooKeeper()`
# unconditionally. No `allow_experimental_transactions` config is needed - part loading never
# goes through `Context::checkTransactionsAreAllowed`.
node = cluster.add_instance("node", with_zookeeper=True)

# On-disk transaction metadata of a rolled-back part, byte for byte (no trailing newline).
# `storing_version` is required: without it the old-format fallback overrides `creation_csn` with
# `Tx::NonTransactionalCSN` and the rollback is silently lost. `creation_csn = Tx::RolledBackCSN`
# lets `read_txn_status` decide rollback without consulting `TransactionLog`. `local_tid` must be
# outside the reserved range (> `Tx::MaxReservedLocalTID` = 32) so the TID is transactional and
# well-formed; `local_tid = 1` is `Tx::NonTransactionalLocalTID` and trips a `chassert` in
# debug/sanitizer builds.
ROLLED_BACK_TXN_VERSION = (
    "version: 1\n"
    "storing_version: 0\n"
    "creation_tid: (2, 33, 00000000-0000-0000-0000-000000000000)\n"
    "creation_csn: 18446744073709551615\n"
    "removal_tid: (0, 0, 00000000-0000-0000-0000-000000000000)\n"
    "removal_csn: 0"
)

# A plausible in-flight record for the tmp-only layout. Its content is irrelevant: rollback is
# decided purely from the presence of `txn_version.txt.tmp` without a final `txn_version.txt`.
IN_FLIGHT_TXN_VERSION = ROLLED_BACK_TXN_VERSION.replace(
    "creation_csn: 18446744073709551615", "creation_csn: 0"
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        # Background merges would rewrite the part set these tests assert on.
        node.query("SYSTEM STOP MERGES")
        yield cluster
    finally:
        cluster.shutdown()


def table_data_path(table):
    """Data directory of an attached table, with a trailing slash."""
    path = node.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables"
        f" WHERE database = 'default' AND name = '{table}'"
    ).strip()
    assert path
    return path.rstrip("/") + "/"


def write_file(path, content):
    """Write `content` to `path` inside the container with no trailing newline added."""
    node.exec_in_container(
        ["bash", "-c", f"printf '%s' '{content}' > {path}"], privileged=True
    )
    assert node.exec_in_container(["bash", "-c", f"cat {path}"]) == content


def fabricate_part(data_path, source, name, txn_version=None, txn_version_tmp=None):
    """
    Clone the committed part `source` under the fabricated part name `name` and optionally give it
    raw transaction metadata. Cloned parts carry no `txn_version.txt`, so `read_txn_status` reports
    `NoMetadata` for them, i.e. non-transactional and committed.

    No API can produce these on-disk states: a part whose creating transaction never committed only
    exists after a crash, and the tmp-only variant only exists when the write was interrupted
    mid-rename. That is why this is an integration test - it owns the server and its disk layout
    instead of assuming the parts live on a local POSIX disk.
    """
    part_path = f"{data_path}{name}"
    node.exec_in_container(
        ["bash", "-c", f"cp -r {data_path}{source} {part_path}"], privileged=True
    )
    if txn_version is not None:
        write_file(f"{part_path}/txn_version.txt", txn_version)
    if txn_version_tmp is not None:
        write_file(f"{part_path}/txn_version.txt.tmp", txn_version_tmp)

    listing = node.exec_in_container(["bash", "-c", f"ls {part_path}"]).split()
    assert ("txn_version.txt" in listing) == (txn_version is not None)
    assert ("txn_version.txt.tmp" in listing) == (txn_version_tmp is not None)


def active_parts(table):
    return set(
        node.query(
            "SELECT name FROM system.parts"
            f" WHERE database = 'default' AND table = '{table}' AND active"
        ).split()
    )


def create_table_with_one_part(table):
    """Create `table`, commit one part `all_1_1_0`, then detach it so its directory can be edited."""
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(f"CREATE TABLE {table} (x UInt32) ENGINE = MergeTree ORDER BY x")
    node.query(f"INSERT INTO {table} VALUES (42)")
    data_path = table_data_path(table)
    node.query(f"DETACH TABLE {table}")
    return data_path


def test_reparent(started_cluster):
    """
    Children of a rolled-back node must be re-inserted at the current tree level, not dropped.

    Reaches the intersection arm of `PartLoadingTree::add` (`!prev_info.isDisjoint(info)`): the
    incoming committed `all_2_3_1_0` intersects the rolled-back `all_1_2_2_1`, which is evicted
    together with re-insertion of its orphans.

    Insertion order inside `PartLoadingTree::build` is (level, mutation) descending:
      1. all_1_2_2_1  level 2, mut 1, blocks 1-2  rolled back
      2. all_1_1_1_1  level 1, mut 1, blocks 1-1  committed, contained in 1-2 -> child of (1)
      3. all_2_3_1_0  level 1, mut 0, blocks 2-3  committed, intersects 1-2 -> evicts (1)
      4. all_1_1_0    level 0, mut 0, blocks 1-1  the original insert, covered by all_1_1_1_1

    The rolled-back parent needs mutation >= the child's, otherwise `contains` returns false,
    `all_1_1_1_1` never becomes its child and the reinsertion path is not exercised at all.
    """
    table = "t_plt_reparent"
    data_path = create_table_with_one_part(table)

    fabricate_part(
        data_path, "all_1_1_0", "all_1_2_2_1", txn_version=ROLLED_BACK_TXN_VERSION
    )
    fabricate_part(data_path, "all_1_1_0", "all_1_1_1_1")
    fabricate_part(data_path, "all_1_1_0", "all_2_3_1_0")

    node.query(f"ATTACH TABLE {table}")

    # all_1_1_1_1 was re-parented to the root and covers the original all_1_1_0.
    assert active_parts(table) == {"all_1_1_1_1", "all_2_3_1_0"}

    node.query(f"DROP TABLE {table} SYNC")


def test_contains(started_cluster):
    """
    A rolled-back part that *contains* its committed peers must not keep them inactive.

    This is a different mechanism from the other three scenarios: `all_1_2_1_0` and `all_3_4_1_0`
    are contained in `all_1_4_2_1`, so `PartLoadingTree::add` takes the containment arm
    (`prev_info.contains(info)`), which only descends - it never calls `read_txn_status` and never
    evicts. The committed descendants are promoted later, when `loadDataPartsFromDisk` demotes the
    rolled-back top-level ancestor to `Outdated`.

    Insertion order:
      1. all_1_4_2_1  level 2, mut 1, blocks 1-4  rolled back
      2. all_1_2_1_0  level 1, mut 0, blocks 1-2  committed, contained in 1-4
      3. all_3_4_1_0  level 1, mut 0, blocks 3-4  committed, contained in 1-4
      4. all_1_1_0    level 0, mut 0, blocks 1-1  the original insert, covered by all_1_2_1_0

    Without the promotion both committed children stay covered by the rolled-back ancestor and are
    invisible to queries.
    """
    table = "t_plt_rb_contains"
    data_path = create_table_with_one_part(table)

    fabricate_part(
        data_path, "all_1_1_0", "all_1_4_2_1", txn_version=ROLLED_BACK_TXN_VERSION
    )
    fabricate_part(data_path, "all_1_1_0", "all_1_2_1_0")
    fabricate_part(data_path, "all_1_1_0", "all_3_4_1_0")

    node.query(f"ATTACH TABLE {table}")

    assert active_parts(table) == {"all_1_2_1_0", "all_3_4_1_0"}

    node.query(f"DROP TABLE {table} SYNC")


def test_evict_reinsert_contains(started_cluster):
    """
    Orphans re-inserted after an eviction must be ordered so that a container is added before the
    part it contains.

    Reaches the intersection arm, and specifically the (level, mutation) descending sort in
    `evict_and_reinsert`: `all_2_4_2_0` contains `all_2_3_1_0`, and adding the container second
    would hit the generic intersection branch (which does not handle `incoming.contains(existing)`)
    and throw a LOGICAL_ERROR.

    Insertion order:
      1. all_1_5_4_1  level 4, mut 1, blocks 1-5  rolled back
      2. all_2_4_2_0  level 2, mut 0, blocks 2-4  committed, contained in 1-5, contains 2-3
      3. all_2_3_1_0  level 1, mut 0, blocks 2-3  committed, contained in 2-4
      4. all_5_6_1_0  level 1, mut 0, blocks 5-6  committed, intersects 1-5 -> evicts (1)
      5. all_1_1_0    level 0, mut 0, blocks 1-1  the original insert, disjoint from the rest

    After the eviction: all_2_4_2_0 and all_5_6_1_0 (and the disjoint all_1_1_0) are active, while
    all_2_3_1_0 is covered by the active container all_2_4_2_0.
    """
    table = "t_plt_evict_reinsert"
    data_path = create_table_with_one_part(table)

    fabricate_part(
        data_path, "all_1_1_0", "all_1_5_4_1", txn_version=ROLLED_BACK_TXN_VERSION
    )
    fabricate_part(data_path, "all_1_1_0", "all_2_4_2_0")
    fabricate_part(data_path, "all_1_1_0", "all_2_3_1_0")
    fabricate_part(data_path, "all_1_1_0", "all_5_6_1_0")

    node.query(f"ATTACH TABLE {table}")

    assert active_parts(table) == {"all_1_1_0", "all_2_4_2_0", "all_5_6_1_0"}

    node.query(f"DROP TABLE {table} SYNC")


def test_tmp_metadata(started_cluster):
    """
    A part that has only a `txn_version.txt.tmp` is rolled back: the creating transaction was
    interrupted before it could rename its metadata into place, so it never committed.
    `read_txn_status` has to mirror `VersionMetadataOnDisk::loadMetadata` here - probing only the
    final `txn_version.txt` reports `NoMetadata`, and the intersecting committed peer then falls
    through to the generic intersecting-parts LOGICAL_ERROR during ATTACH.

    Reaches the intersection arm.

    Insertion order:
      1. all_1_2_1_0  level 1, mut 0, blocks 1-2  rolled back (tmp-only metadata)
      2. all_2_3_0_0  level 0, mut 0, blocks 2-3  committed, intersects 1-2 -> evicts (1)
      3. all_1_1_0    level 0, mut 0, blocks 1-1  the original insert, disjoint from 2-3
    """
    table = "t_plt_tmp_metadata"
    data_path = create_table_with_one_part(table)

    fabricate_part(
        data_path,
        "all_1_1_0",
        "all_1_2_1_0",
        txn_version_tmp=IN_FLIGHT_TXN_VERSION,
    )
    fabricate_part(data_path, "all_1_1_0", "all_2_3_0_0")

    node.query(f"ATTACH TABLE {table}")

    assert active_parts(table) == {"all_1_1_0", "all_2_3_0_0"}

    node.query(f"DROP TABLE {table} SYNC")


# The symmetric `next`/`isDisjoint` branch of `PartLoadingTree::add` is a near-duplicate of the
# `prev` branch exercised above, and no test targets it directly - the scenarios happen to reach
# the `prev` side because of the insertion order. Same for the `Unreadable`/CORRUPTED_DATA and
# `UnknownCSN` outcomes of `read_txn_status`. Both gaps predate this module.
