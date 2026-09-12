import itertools
import random
import string

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# ZooKeeper is required: classifying a part whose `txn_version.txt` carries a transactional TID
# reaches `TransactionLog::instance`, whose constructor calls `loadLogFromZooKeeper`
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
        yield cluster
    finally:
        cluster.shutdown()


def table_data_path(table, part):
    """
    Directory holding the parts of `table`, with a trailing slash, derived from the directory the
    server actually wrote `part` into.

    `system.parts.path` rather than `system.tables.data_paths`: `path` is
    `DataPartStorageOnDiskBase::getFullPath` for that one part, i.e. the exact directory the
    fabricated siblings must be created next to. `data_paths` is an array over the whole storage
    policy, so `arrayElement(data_paths, 1)` picks a volume rather than the part's own location and
    silently addresses the wrong directory once a table has more than one data path.
    """
    path = node.query(
        "SELECT path FROM system.parts"
        f" WHERE database = 'default' AND table = '{table}'"
        f" AND name = '{part}' AND active"
    ).strip()
    assert path
    parent, _, part_dir = path.rstrip("/").rpartition("/")
    # Fail loudly rather than fabricating parts in some parent of the real directory.
    assert part_dir == part, f"unexpected part directory {part_dir} for part {part}"
    return parent + "/"


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


def all_parts(table):
    """Every loaded part with its active flag, including `Outdated` ones."""
    node.query(f"SYSTEM WAIT LOADING PARTS {table}")
    rows = node.query(
        "SELECT name, active FROM system.parts"
        f" WHERE database = 'default' AND table = '{table}'"
    ).split()
    return dict(zip(rows[::2], (r == "1" for r in rows[1::2])))


def stop_merges(table):
    """
    Redundant second line of defence only. The guard that actually prevents merges is
    `max_bytes_to_merge_at_max_space_in_pool = 0` in the table metadata, see
    `create_table_with_one_part`.

    This statement cannot protect the window inside `ATTACH TABLE`: `ATTACH` runs
    `IStorage::startup` before returning (`InterpreterCreateQuery.cpp`), and
    `StorageMergeTree::startup` schedules the background assignee immediately, so a merge can be
    selected before any statement issued after `ATTACH` reaches the server.

    A global `SYSTEM STOP MERGES` would be worse still: it only locks the tables that exist when it
    runs (`InterpreterSystemQuery::startStopAction`, lock keyed per `IStorage`), so it would not
    cover a table created afterwards, and the lock does not survive the `DETACH`/`ATTACH` cycle
    that destroys the storage instance.
    """
    node.query(f"SYSTEM STOP MERGES {table}")


def recover_detached_table(table):
    """
    Recover `table` if a previous failed run left it in `system.detached_tables`.

    Every test here fails between `DETACH TABLE` and the final `DROP TABLE` when its `ATTACH`
    throws, and the table then stays detached: `DROP TABLE IF EXISTS` only sees attached tables,
    while the detached metadata still blocks `CREATE TABLE`
    (`DatabaseOnDisk::checkMetadataFilenameAvailabilityUnlocked` throws
    `TABLE_ALREADY_EXISTS ... (detached)`). Repeated runs (`pytest --count`, flaky-check) reuse the
    module-scoped cluster, so without this recovery the first real failure would cascade into
    setup failures in every later iteration instead of independent reproductions.

    The fabricated part directories are removed before `ATTACH`: they are what made the previous
    `ATTACH` throw, so re-attaching them would fail the same way. Every part of these tables lives
    in the single `all` partition, hence the `all_*` pattern. The `store/xxx/<uuid>/` layout is the
    one `DatabaseAtomic` uses on the local `default` disk the tables are pinned to.
    """
    uuid = node.query(
        "SELECT uuid FROM system.detached_tables"
        f" WHERE database = 'default' AND table = '{table}'"
    ).strip()
    if not uuid:
        return
    data_path = f"/var/lib/clickhouse/store/{uuid[:3]}/{uuid}/"
    node.exec_in_container(["bash", "-c", f"rm -rf {data_path}all_*"], privileged=True)
    node.query(f"ATTACH TABLE {table}")
    node.query(f"DROP TABLE {table} SYNC")


def create_table_with_one_part(table):
    """
    Create `table`, commit one part `all_1_1_0`, then detach it so its directory can be edited.

    Merges are disabled in the table metadata rather than by a statement, so the storage comes up
    with merging already off on every startup, including the one `ATTACH TABLE` performs before it
    returns. `max_bytes_to_merge_at_max_space_in_pool = 0` is checked before any merge selector runs
    (`getMaxSourcePartsBytesForMerge` returns 0, `StorageMergeTree` reports `CANNOT_SELECT` with
    `Current value of max_source_parts_bytes is zero`). A merge would rewrite the part set these
    tests assert on.

    `storage_policy = 'default'` is pinned rather than inherited: the fabricated `txn_version.txt`
    below is a raw plaintext file, which only parses on a local disk (on an object-storage disk every
    file in a part directory has to be in `DiskObjectStorageMetadata` format). That assumption is
    what the stateless tests left implicit, so it is stated here instead of taken from whatever
    default policy a job's config happens to expose.

    `DETACH` must be `SYNC`: an asynchronous detach leaves the storage instance tracked in
    `DatabaseAtomic::detached_tables` while another subsystem still holds a `StoragePtr`
    (`ServerAsynchronousMetrics` iterates a snapshot of them), and the later `ATTACH` then throws
    `TABLE_ALREADY_EXISTS` rather than waiting.
    """
    recover_detached_table(table)
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"CREATE TABLE {table} (x UInt32) ENGINE = MergeTree ORDER BY x"
        " SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0,"
        " storage_policy = 'default'"
    )
    stop_merges(table)
    node.query(f"INSERT INTO {table} VALUES (42)")
    data_path = table_data_path(table, "all_1_1_0")
    node.query(f"DETACH TABLE {table} SYNC")
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
    stop_merges(table)

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

    `all_1_2_1_0` and `all_3_4_1_0` share (level, mutation) = (1, 0) and `PartLoadingTree::build`
    sorts with a non-stable `std::sort`, so steps 2 and 3 may swap. Both are contained in
    `all_1_4_2_1` and disjoint from each other, so either order puts both under it and the asserted
    promotion is the same.

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
    stop_merges(table)

    assert active_parts(table) == {"all_1_2_1_0", "all_3_4_1_0"}

    node.query(f"DROP TABLE {table} SYNC")


def test_evict_reinsert_contains(started_cluster):
    """
    Evicting a rolled-back node that has a *nested* committed subtree must keep every orphan: the
    container `all_2_4_2_0` active, and the part it contains, `all_2_3_1_0`, covered by it rather
    than dropped.

    Reaches the intersection arm. Insertion order:
      1. all_1_5_4_1  level 4, mut 1, blocks 1-5  rolled back
      2. all_2_4_2_0  level 2, mut 0, blocks 2-4  committed, contained in 1-5, contains 2-3
      3. all_2_3_1_0  level 1, mut 0, blocks 2-3  committed, contained in 2-4
      4. all_5_6_1_0  level 1, mut 0, blocks 5-6  committed, intersects 1-5 -> evicts (1)
      5. all_1_1_0    level 0, mut 0, blocks 1-1  the original insert, disjoint from the rest

    This does NOT pin the (level, mutation) descending sort of `evict_and_reinsert`
    (`MergeTreeData.cpp:2167-2171`): `collect` walks `children`, a `std::map` keyed on
    `MergeTreePartInfo`, so it already yields the container 2-4 before the part 2-3 it contains, and
    the incoming 5-6 is disjoint from both. Removing that sort leaves this outcome unchanged.

    `all_2_3_1_0` and `all_5_6_1_0` share (level, mutation) = (1, 0) and `PartLoadingTree::build`
    sorts with a non-stable `std::sort`, so steps 3 and 4 may swap. The asserted outcome holds for
    either order: with 5-6 first the eviction fires before 2-3 joins the victim's subtree, and 2-3
    then arrives through the containment arm under the already-reinserted 2-4.
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
    stop_merges(table)

    # all_2_3_1_0 must be present but covered by the reinserted container all_2_4_2_0, not dropped:
    # a regression that loses the orphan is invisible to an active-only check.
    assert all_parts(table) == {
        "all_1_1_0": True,
        "all_2_4_2_0": True,
        "all_5_6_1_0": True,
        "all_2_3_1_0": False,
    }

    node.query(f"DROP TABLE {table} SYNC")


def test_tmp_metadata(started_cluster):
    """
    A part that has only a `txn_version.txt.tmp` is rolled back: the creating transaction was
    interrupted before it could rename its metadata into place, so it never committed.
    `read_txn_status` has to mirror `VersionMetadataOnDisk::loadMetadata` here - probing only the
    final `txn_version.txt` reports `NoMetadata`, and the intersecting committed peer then falls
    through to the generic intersecting-parts `LOGICAL_ERROR` during `ATTACH`.

    Reaches the intersection arm.

    Insertion order:
      1. all_1_2_1_0  level 1, mut 0, blocks 1-2  rolled back (tmp-only metadata)
      2. all_2_3_0_0  level 0, mut 0, blocks 2-3  committed, intersects 1-2 -> evicts (1)
      3. all_1_1_0    level 0, mut 0, blocks 1-1  the original insert, disjoint from 2-3

    `all_2_3_0_0` and `all_1_1_0` share (level, mutation) = (0, 0) and `PartLoadingTree::build`
    sorts with a non-stable `std::sort`, so steps 2 and 3 may swap. The asserted outcome holds for
    either order: with 1-1 first it is contained in 1-2 and becomes a child of the rolled-back node,
    and the eviction that 2-3 then triggers reinserts it as an orphan at the root.
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
    stop_merges(table)

    assert active_parts(table) == {"all_1_1_0", "all_2_3_0_0"}

    node.query(f"DROP TABLE {table} SYNC")


# The symmetric `next`/`isDisjoint` branch of `PartLoadingTree::add` is a near-duplicate of the
# `prev` branch exercised above, and no test targets it directly - the scenarios happen to reach
# the `prev` side because of the insertion order. Same for the `Unreadable`/`CORRUPTED_DATA` and
# `UnknownCSN` outcomes of `read_txn_status`. Both gaps predate this module.


# The tests above load parts through `ATTACH TABLE`, i.e. `loadDataPartsFromDisk`. `SYSTEM RESTART
# DISK` instead re-scans an already-loaded table whose every disk is read-only, through
# `MergeTreeData::refreshDataPartsOnce`, which seeds `PartLoadingTree` itself. The promotion of
# committed descendants of a rolled-back or broken covering part therefore has to be asserted
# separately there, including across two refreshes: the covering part is then either already
# indexed non-active (a rolled-back one) or not indexed at all (a broken one), and either way the
# seed has to reach the descendants that appeared only afterwards.
#
# `object_storage_type = local` with `metadata_type = plain_rewritable` is what makes a read-only
# table's store writable from outside the server: plain_rewritable keeps every part file as a plain
# file, so the raw `txn_version.txt` above still parses, and it maps a logical part name to a
# directory through `__meta/<dir>/prefix.path`, so a part can be published by moving a directory in
# and adding that one mapping file. `table_disk = true` puts the parts at the disk root rather than
# under `store/<uuid>/`, which is what lets a fresh reader be pointed at a fabricated layout.
REFRESH_DISK_ROOT = "/var/lib/clickhouse/plt_refresh"

# Fresh disk name and directory per reader: a custom disk is cached by name for the lifetime of the
# server (`Context::getOrCreateDisk`), so reusing a name across tests would hand the second one the
# first one's disk object and path map. Repeated runs against the module-scoped cluster
# (`pytest --count`, flaky check) reuse the same server, so the counter has to live here.
reader_seq = itertools.count()


def container_bash(command):
    """Run `command` in the node's container as root, returning its output."""
    return node.exec_in_container(["bash", "-c", command], privileged=True)


def part_states(table):
    """
    Every part `table` has in its parts index, with its state.

    `_state` has to be in the SELECT list: without that virtual column `system.parts` reports at
    most `Active` and `Outdated` parts (`StoragesInfo::getParts`), and a rolled-back part on a
    read-only table is in neither state - every refresh ends in `grabOldParts(true)`, which moves it
    to `Deleting`, while nothing on a read-only table ever finishes the removal that would take it
    out of the index.
    """
    rows = node.query(
        "SELECT name, _state FROM system.parts"
        f" WHERE database = 'default' AND table = '{table}'"
    ).split()
    return dict(zip(rows[::2], rows[1::2]))


def object_dir_of(store, part):
    """
    The directory `plain_rewritable` mapped the logical part name `part` to, read from the
    `__meta/<dir>/prefix.path` files that hold the mapping.

    `system.parts.path` cannot answer this: it reports the logical path (`<disk root>/<part>/`),
    which is the key of the mapping rather than the directory the files are in.
    """
    # `-x` still matches although the mapping files carry no trailing newline, and `|| true`
    # keeps a missing mapping an assertion below rather than an opaque non-zero exit code.
    found = container_bash(
        f"grep -Fxl '{part}/' {store}__meta/*/prefix.path || true"
    ).split()
    assert len(found) == 1, f"expected one directory mapped to {part}, got {found}"
    return found[0].rsplit("/", 2)[-2]


@pytest.fixture(scope="module")
def committed_part_copy(started_cluster):
    """
    A copy of one committed part directory, written by the server on a `plain_rewritable` disk.

    The parts fabricated below are clones of it, so they carry the layout the server itself wrote
    and, having no `txn_version.txt`, are classified `NoMetadata`, i.e. committed. The writer table
    is dropped once the copy exists - the readers get their own empty layouts, and a part is
    injected only after a reader is loaded, which is what leaves the refresh path, rather than the
    startup loader, responsible for surfacing it.
    """
    container_bash(f"rm -rf {REFRESH_DISK_ROOT} && mkdir -p {REFRESH_DISK_ROOT}")
    node.query("DROP TABLE IF EXISTS plt_refresh_writer SYNC")
    node.query(
        "CREATE TABLE plt_refresh_writer (x UInt32) ENGINE = MergeTree ORDER BY x"
        " SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, table_disk = true,"
        " disk = disk(name = plt_refresh_writer, type = object_storage,"
        " object_storage_type = local, metadata_type = plain_rewritable,"
        f" path = '{REFRESH_DISK_ROOT}/writer/')"
    )
    node.query("INSERT INTO plt_refresh_writer VALUES (42)")
    store = f"{REFRESH_DISK_ROOT}/writer/"
    source = f"{REFRESH_DISK_ROOT}/source"
    container_bash(f"cp -r {store}{object_dir_of(store, 'all_1_1_0')} {source}")
    node.query("DROP TABLE plt_refresh_writer SYNC")
    yield source
    container_bash(f"rm -rf {REFRESH_DISK_ROOT}")


def stage_part(source, part, rolled_back=False, broken=False):
    """
    Clone `source` into a staging directory as the part named `part`, ready to be injected.

    `rolled_back` gives it the raw metadata of a part whose transaction never committed. `broken`
    corrupts `columns.txt` so `loadDataPart` fails to parse it and marks the part broken - a broken
    part is never inserted into the parts index, unlike a rolled-back one, which is indexed
    `Outdated`.
    """
    object_dir = "".join(random.choices(string.ascii_lowercase, k=32))
    staged = f"{REFRESH_DISK_ROOT}/staged/{object_dir}"
    container_bash(f"mkdir -p {REFRESH_DISK_ROOT}/staged && cp -r {source} {staged}")
    if rolled_back:
        write_file(f"{staged}/txn_version.txt", ROLLED_BACK_TXN_VERSION)
    if broken:
        write_file(f"{staged}/columns.txt", "corrupted columns metadata")
    return part, object_dir


def inject_part(store, staged_part):
    """
    Publish a staged part into a loaded read-only reader, the way another writer process would: move
    its directory into the store and add the one `__meta` file that maps the part name to it.
    """
    part, object_dir = staged_part
    container_bash(
        f"mv {REFRESH_DISK_ROOT}/staged/{object_dir} {store}{object_dir}"
        f" && mkdir -p {store}__meta/{object_dir}"
        f" && printf '%s/' '{part}' > {store}__meta/{object_dir}/prefix.path"
    )


def create_readonly_reader(name):
    """
    Create a table over a fresh, empty `plain_rewritable` layout on a read-only disk, and return its
    name, its disk name and its store directory.

    The table is loaded while the layout is still empty, so every part asserted on afterwards is one
    that `refreshDataPartsOnce` had to surface. `data_paths[1]` is the whole store here because
    `table_disk = true` gives the table exactly one data path.
    """
    suffix = f"{name}_{next(reader_seq)}"
    table = f"plt_refresh_{suffix}"
    disk = f"plt_refresh_disk_{suffix}"
    container_bash(f"mkdir -p {REFRESH_DISK_ROOT}/{suffix}/__meta")
    node.query(
        f"CREATE TABLE {table} (x UInt32) ENGINE = MergeTree ORDER BY x"
        " SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, table_disk = true,"
        f" disk = disk(readonly = true, name = {disk}, type = object_storage,"
        " object_storage_type = local, metadata_type = plain_rewritable,"
        f" path = '{REFRESH_DISK_ROOT}/{suffix}/')"
    )
    store = node.query(
        "SELECT data_paths[1] FROM system.tables"
        f" WHERE database = 'default' AND name = '{table}'"
    ).strip()
    assert store
    return table, disk, store.rstrip("/") + "/"


def test_refresh_disk_contains(committed_part_copy):
    """
    A read-only refresh must promote the committed descendants of a rolled-back covering part, and
    must not re-activate the covering part itself.

    Same topology and containment arm as `test_contains`, but the parts appear after the table is
    loaded, so they are surfaced by `refreshDataPartsOnce`, not by the startup loader:
      all_1_4_2_1  level 2, mut 1, blocks 1-4  rolled back
      all_1_2_1_0  level 1, mut 0, blocks 1-2  committed, contained in 1-4
      all_3_4_1_0  level 1, mut 0, blocks 3-4  committed, contained in 1-4

    Committing the rolled-back top-level node instead of skipping it puts it back into `PreActive`
    and then throws `LOGICAL_ERROR` out of `assertHasVersionMetadata`, which accepts only a
    non-transactional creation TID under the null transaction the refresh commits with. The refresh
    therefore fails, the committed children stay hidden, and a debug or sanitizer build aborts.
    """
    table, disk, store = create_readonly_reader("contains")

    inject_part(store, stage_part(committed_part_copy, "all_1_4_2_1", rolled_back=True))
    inject_part(store, stage_part(committed_part_copy, "all_1_2_1_0"))
    inject_part(store, stage_part(committed_part_copy, "all_3_4_1_0"))
    node.query(f"SYSTEM RESTART DISK {disk}")

    assert active_parts(table) == {"all_1_2_1_0", "all_3_4_1_0"}

    node.query(f"DROP TABLE {table} SYNC")


def test_refresh_disk_contains_across_refreshes(committed_part_copy):
    """
    The committed descendants must be surfaced even when they appear only after the rolled-back
    covering part has already been indexed by an earlier refresh.

    The first refresh sees only `all_1_4_2_1` and indexes it `Outdated`. A read-only table never
    starts the old-part cleanup thread (`StorageMergeTree::startup` returns before it), so the part
    stays in the index. The seed of the second refresh must therefore descend through a top-level
    node that is already indexed but not active, otherwise the children that appeared in between
    stay invisible until the table is restarted or re-attached.
    """
    table, disk, store = create_readonly_reader("cross_refresh")

    inject_part(store, stage_part(committed_part_copy, "all_1_4_2_1", rolled_back=True))
    node.query(f"SYSTEM RESTART DISK {disk}")
    # In the index and not active: this is the state the next refresh has to look past, so asserting
    # the part is still there is what keeps the second half of the test from passing vacuously.
    assert part_states(table) == {"all_1_4_2_1": "Deleting"}

    inject_part(store, stage_part(committed_part_copy, "all_1_2_1_0"))
    inject_part(store, stage_part(committed_part_copy, "all_3_4_1_0"))
    node.query(f"SYSTEM RESTART DISK {disk}")

    assert active_parts(table) == {"all_1_2_1_0", "all_3_4_1_0"}

    node.query(f"DROP TABLE {table} SYNC")


def test_refresh_disk_broken_covering_across_refreshes(committed_part_copy):
    """
    A committed part must be surfaced when it appears under a broken covering part whose own child
    is already indexed from an earlier refresh.

    Topology all_1_8_3_1 (broken) > all_1_4_2_1 (rolled back) > all_1_2_1_0 (committed). A broken
    part never reaches the parts index at all (`loadDataPart` returns through `mark_broken` before
    the insert), so on the second refresh the top-level seed re-visits it as an unknown node - and
    has to descend into its subtree past `all_1_4_2_1`, which the first refresh did index
    `Outdated`, to reach the grandchild that appeared afterwards. Handling only the direct children
    of a broken node, or stopping at any indexed node, hides the grandchild until a restart.
    """
    table, disk, store = create_readonly_reader("broken_covering")

    inject_part(store, stage_part(committed_part_copy, "all_1_8_3_1", broken=True))
    inject_part(store, stage_part(committed_part_copy, "all_1_4_2_1", rolled_back=True))
    node.query(f"SYSTEM RESTART DISK {disk}")
    # The broken covering part never reached the index; the rolled-back one is in it, not active.
    # Both halves of that are what the next refresh has to handle, so both are asserted here.
    assert part_states(table) == {"all_1_4_2_1": "Deleting"}

    inject_part(store, stage_part(committed_part_copy, "all_1_2_1_0"))
    node.query(f"SYSTEM RESTART DISK {disk}")

    assert active_parts(table) == {"all_1_2_1_0"}

    node.query(f"DROP TABLE {table} SYNC")
