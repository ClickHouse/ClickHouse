import os
import time

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_conf.xml"],
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


# ==============================================================================
# Shared helpers
# ==============================================================================
def setup_table(name, extra_settings="", n=node):
    n.query(f"DROP TABLE IF EXISTS {name} SYNC")
    n.query("SYSTEM STOP MERGES")  # keep a single, predictable part
    settings = "min_bytes_for_wide_part = 0"
    if extra_settings:
        settings += ", " + extra_settings
    n.query(
        f"""CREATE TABLE {name} (key UInt64, id UInt64, value String,
            PROJECTION p (SELECT key, id, value ORDER BY id))
            ENGINE = MergeTree ORDER BY key SETTINGS {settings}"""
    )
    n.query(
        f"INSERT INTO {name} SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )


def part_dir(name, n=node):  # absolute container path, no trailing slash: .../all_1_1_0
    return (
        n.query(
            f"SELECT path FROM system.parts WHERE table = '{name}' AND active = 1 LIMIT 1"
        )
        .strip()
        .rstrip("/")
    )


def part_name(name, n=node):
    return part_dir(name, n).split("/")[-1]


def proj_query(name, n=node, extra_settings=""):
    settings = "optimize_use_projections = 1"
    if extra_settings:
        settings += ", " + extra_settings
    return n.query(
        f"SELECT count(), sum(key) FROM {name} WHERE id < 200 SETTINGS {settings}"
    ).strip()


def check_table(name, n=node):
    return n.query(
        f"CHECK TABLE {name} SETTINGS check_query_single_value_result = 1"
    ).strip()


def path_exists(p, n=node):
    return (
        n.exec_in_container(
            ["bash", "-c", f"test -e {p} && echo 1 || echo 0"],
            privileged=True,
            user="root",
        ).strip()
        == "1"
    )


def active_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.parts WHERE table = '{name}' AND active = 1"
    ).strip()


def active_projection_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.projection_parts WHERE table = '{name}' AND active = 1"
    ).strip()


def broken_projection_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.projection_parts WHERE table = '{name}' AND is_broken"
    ).strip()


def wait_for(predicate, timeout=60):
    for _ in range(timeout * 2):
        if predicate():
            return
        time.sleep(0.5)
    raise AssertionError(f"wait_for timed out after {timeout}s")


def block_until_tables_loaded(name, n=node):
    # Reads the real table to wait out its async load (async_load_databases = 1); a system.parts read does not.
    # Call after a (re)start before asserting on system.* so the assert does not race the background loader.
    n.query(f"SELECT count() FROM {name}")


# Table-level settings clause that forces packed part storage regardless of part size.
PACKED = "min_bytes_for_full_part_storage = '1G'"


def packed_archive_member(part_path, member, n=node, disk="default"):
    """Content of one member file of the data.packed archive at part_path."""
    root = n.query(f"SELECT path FROM system.disks WHERE name = '{disk}'").strip()
    rel = os.path.relpath(os.path.join(part_path, "data.packed"), root)
    out_rel = "tmp_packed_extract"
    out_abs = os.path.join(root, out_rel)
    n.exec_in_container(["bash", "-c", f"rm -rf {out_abs}"], privileged=True, user="root")
    n.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse disks --config /etc/clickhouse-server/config.xml --disk {disk}"
            f' --query "packed-io extract --disk-from {disk} {rel} {out_rel}"',
        ],
        privileged=True,
        user="root",
    )
    content = n.exec_in_container(
        ["bash", "-c", f"cat {out_abs}/{member} && rm -rf {out_abs}"],
        privileged=True,
        user="root",
    ).strip()
    return content


# Storage kinds a test runs for; 'packed' appends the packed-threshold clause to the table settings.
@pytest.fixture(params=["full", "packed"])
def storage_kind(request):
    return request.param


def with_storage(extra_settings, storage_kind):
    return extra_settings + (", " + PACKED if storage_kind == "packed" else "")


# Disk backend a mutation/rebuild test runs for. 'local' is the default disk (immediate part
# transactions); 's3' is the MinIO-backed disk, which runs DEFERRED transactions -- the setting the
# object-storage projection-rebuild regression needs. See section N. (Azure/Azurite exercises the same
# deferred-transaction path; it was verified manually but kept out of the automated matrix because a
# per-worker azurite cluster times out under xdist parallelism -- s3 covers the regression.)
@pytest.fixture(params=["local", "s3"])
def disk(request):
    return request.param


def with_disk(extra_settings, disk):
    return extra_settings + (f", storage_policy = '{disk}'" if disk != "local" else "")


# ==============================================================================
# A. Layout basics
# ==============================================================================

# This test pins the projection on-disk layout: the projection is stored nested inside its parent
# part directory, and no sibling directory ever appears at the parts root.
# Scenario:
# - create table with a projection
# - assert structure: the projection is nested inside the part, no sibling at the parts root
# - assert SELECT through the projection
# - restart, assert SELECT unchanged and the part is intact
def test_layout_nested():
    setup_table("t_nested")

    # assert structure: nested inside the part, no sibling at the parts root
    p = part_dir("t_nested")
    assert path_exists(f"{p}/p.proj")
    assert not path_exists(f"{p}.p.proj")

    # assert SELECT through the projection
    baseline = proj_query("t_nested")

    # restart, assert SELECT unchanged and the part intact
    node.restart_clickhouse()
    assert proj_query("t_nested") == baseline
    assert active_parts("t_nested") == "1"


# ==============================================================================
# F. Manifest desync, repair, and adoption policy
# ==============================================================================

# A projection dir present on disk and declared in metadata but missing from checksums.txt is NOT
# adopted: checksums.txt is the commit point, loadProjections loads only what the manifest lists.


def _make_desync_pair(
    prefix,
    extra="",
    donor_rows=1000,
    donor_offset=0,
    projection="p (SELECT key, id ORDER BY id)",
    donor_projection=None,
    donor_id_type="UInt64",
):
    """Victim table whose part has NO projection dir/record + a donor providing a real dir.
    The donor knobs shape the planted dir: row count/values, projection definition, column type."""
    donor_projection = donor_projection or projection
    node.query(f"DROP TABLE IF EXISTS {prefix} SYNC")
    node.query(f"DROP TABLE IF EXISTS {prefix}_donor SYNC")
    node.query("SYSTEM STOP MERGES")
    settings = "min_bytes_for_wide_part = 0"
    if extra:
        settings += ", " + extra
    node.query(
        f"""CREATE TABLE {prefix} (key UInt64, id UInt64, value String,
           PROJECTION {projection})
           ENGINE = MergeTree ORDER BY key
           SETTINGS {settings}, materialize_projections_on_insert = 0"""
    )
    node.query(
        f"""CREATE TABLE {prefix}_donor (key UInt64, id {donor_id_type}, value String,
           PROJECTION {donor_projection})
           ENGINE = MergeTree ORDER BY key
           SETTINGS {settings}"""
    )
    node.query(
        f"INSERT INTO {prefix} SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    node.query(
        f"INSERT INTO {prefix}_donor SELECT number, number * 2, toString(number) FROM numbers({donor_offset}, {donor_rows})"
    )


def _manifest_mentions_projection(part_path):
    return (
        node.exec_in_container(
            ["bash", "-c", f"grep -aco 'p\\.proj' {part_path}/checksums.txt || true"],
            privileged=True,
            user="root",
        ).strip()
        != "0"
    )


# This test checks that regenerating a lost manifest restores projection records: without the fix
# checkDataPart folds them only from the (empty-during-repair) projection map and drops every projection.
# Scenario:
# - create table with a projection, capture SELECT baseline
# - stop the server, delete checksums.txt, start it (manifest is regenerated)
# - assert the data reads, the projection is healthy, SELECT matches, CHECK TABLE passes
# - restart and assert the regenerated manifest still references the projection
def test_desync_repair_regenerates_records():
    # create table with a projection, capture baseline
    setup_table("t_fix")
    baseline = proj_query("t_fix")
    p = part_dir("t_fix")

    # delete checksums.txt so the manifest is regenerated on load
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"rm {p}/checksums.txt"], privileged=True, user="root"
    )
    node.start_clickhouse()

    # assert the data reads, the projection is healthy and served, CHECK passes
    assert node.query("SELECT count() FROM t_fix").strip() == "1000"
    assert broken_projection_parts("t_fix") == "0"
    assert proj_query("t_fix", extra_settings="force_optimize_projection = 1") == baseline
    assert check_table("t_fix") == "1"

    # assert the regenerated manifest references the projection: it must survive a reload
    node.restart_clickhouse()
    block_until_tables_loaded("t_fix")
    assert active_projection_parts("t_fix") == "1"
    assert check_table("t_fix") == "1"


# A projection dir planted inside a part but absent from checksums.txt is not adopted:
# checksums is the commit point. The dir is ignored, the part reads from the base.
# Scenario:
# - build a desync pair whose victim has no projection record
# - stop the server, copy the donor's projection dir into the victim part, start it
# - assert the projection is not adopted (not active, not broken, not in the manifest) and data reads
def test_desync_unlisted_dir_ignored():
    # build a desync pair, then plant the donor's projection dir inside the victim part
    _make_desync_pair("t_ln")
    p = part_dir("t_ln")
    donor_proj = f"{part_dir('t_ln_donor')}/p.proj"
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"cp -r {donor_proj} {p}/p.proj && chmod -R 777 {p}/p.proj"],
        privileged=True,
        user="root",
    )
    node.start_clickhouse()
    block_until_tables_loaded("t_ln")

    # assert the unlisted dir is ignored, not adopted
    assert node.query("SELECT count() FROM t_ln").strip() == "1000"
    assert active_projection_parts("t_ln") == "0"
    assert broken_projection_parts("t_ln") == "0"
    assert not _manifest_mentions_projection(p)


# ==============================================================================
# G. Reload consistency
# ==============================================================================

# This test checks that CHECK TABLE classifies an unknown projection (left after DROP PROJECTION
# on a detached part) as a projection problem, not a broken part.
# Scenario:
# - create table with a projection, DETACH PART, DROP PROJECTION, ATTACH PART
# - run CHECK TABLE
# - assert the result reports "unexpected projection" and the data is still readable
def test_reload_check_table_dropped_projection():
    # create table with a projection, then drop the projection while the part is detached
    setup_table("t_chk")
    name = part_name("t_chk")
    node.query(f"ALTER TABLE t_chk DETACH PART '{name}'")
    node.query("ALTER TABLE t_chk DROP PROJECTION p")
    node.query(f"ALTER TABLE t_chk ATTACH PART '{name}'")

    # assert CHECK TABLE flags the unknown projection and the data stays readable
    result = node.query("CHECK TABLE t_chk SETTINGS check_query_single_value_result = 0")
    assert "unexpected projection" in result, result
    assert node.query("SELECT count() FROM t_chk").strip() == "1000"


# ==============================================================================
# M. Packed part storage
# ==============================================================================


# This test checks that REPLACE PARTITION FROM on packed parts does not apply parent-only
# ClonePartParams to projection sub-parts: metadata_version_to_write must not overwrite the
# projection's own metadata_version.txt (every part, projections included, carries one since
# INSERT), and invalidated_system_columns.txt must not be manufactured inside the projection dir.
# Scenario:
# - create packed replicated src and dst tables with a projection; insert, then bump both tables'
#   metadata version so the src part predates it
# - REPLACE PARTITION on dst from src (this flow sets both parent-only params)
# - assert the dst parent part carries the bumped version + the invalidated-columns file
# - assert the dst projection sub-part keeps the part's original version and has no invalidated file
def test_packed_replace_partition_no_parent_artifacts_in_projection():
    for t, zk in (("t_pk_rp_src", "src"), ("t_pk_rp_dst", "dst")):
        node.query(f"DROP TABLE IF EXISTS {t} SYNC")
        node.query("SYSTEM STOP MERGES")
        node.query(
            f"""CREATE TABLE {t} (key UInt64, id UInt64, value String,
                PROJECTION p (SELECT key, id, value ORDER BY id))
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_pk_rp_{zk}', '1')
                ORDER BY key
                SETTINGS min_bytes_for_wide_part = 0, {PACKED}"""
        )
    node.query(
        "INSERT INTO t_pk_rp_src SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    # Bump both tables to metadata version >= 1 after the insert (identical structures keep REPLACE
    # legal; a comment-only change would not bump the version). The src part stays at version 0, so
    # an override reaching the projection sub-part is distinguishable from the version INSERT wrote.
    for t in ("t_pk_rp_src", "t_pk_rp_dst"):
        node.query(f"ALTER TABLE {t} ADD COLUMN extra UInt8 DEFAULT 0")
    node.query("ALTER TABLE t_pk_rp_dst REPLACE PARTITION ID 'all' FROM t_pk_rp_src")

    # the cloned part is packed and carries its nested packed projection
    p = part_dir("t_pk_rp_dst")
    assert path_exists(f"{p}/data.packed")
    assert path_exists(f"{p}/p.proj/data.packed")

    # parent-only artifacts land at the part level: the overridden version and the invalidated file...
    assert packed_archive_member(p, "metadata_version.txt") != "0"
    assert path_exists(f"{p}/invalidated_system_columns.txt")

    # ...and must not be applied to the projection sub-part
    assert packed_archive_member(f"{p}/p.proj", "metadata_version.txt") == "0"
    assert not path_exists(f"{p}/p.proj/invalidated_system_columns.txt")

    # the projection still serves queries and the part passes CHECK TABLE
    assert proj_query("t_pk_rp_dst") == proj_query("t_pk_rp_src")
    assert check_table("t_pk_rp_dst") == "1"


# This test checks that freeze of a packed part copies projections from the part's owned set instead
# of walking the part dir on disk: residue directories of a failed operation (a live-named dir the
# checksums manifest never adopted, or a *.tmp_proj leftover) must not be carried into the copy.
# Scenario:
# - create a packed table with a projection
# - plant residue dirs inside the live part dir: residue.proj and p_1.tmp_proj
# - FREEZE WITH NAME
# - assert the shadow copy carries p.proj (with its archive) and neither residue dir
def test_packed_freeze_excludes_residue():
    setup_table("t_pk_frz", PACKED)
    p = part_dir("t_pk_frz")
    assert path_exists(f"{p}/data.packed")

    # plant residue: an unadopted live-named dir and a temp leftover, both with a marker file
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {p}/residue.proj {p}/p_1.tmp_proj"
            f" && touch {p}/residue.proj/stale_marker.txt {p}/p_1.tmp_proj/stale_marker.txt"
            f" && chmod -R 777 {p}/residue.proj {p}/p_1.tmp_proj",
        ],
        privileged=True,
        user="root",
    )

    node.query("ALTER TABLE t_pk_frz FREEZE WITH NAME 'pkfrz'")

    # the shadow copy carries the owned projection but no residue
    name = part_name("t_pk_frz")
    shadow = node.exec_in_container(
        ["bash", "-c", f"find /var/lib/clickhouse/shadow/pkfrz -maxdepth 4 -name '{name}' -type d | head -1"],
        privileged=True,
        user="root",
    ).strip()
    assert shadow != ""
    assert path_exists(f"{shadow}/data.packed")
    assert path_exists(f"{shadow}/p.proj/data.packed")
    assert not path_exists(f"{shadow}/residue.proj")
    assert not path_exists(f"{shadow}/p_1.tmp_proj")


# This test checks that DETACH/ATTACH PART carries the nested packed projection sub-part
# (freeze-based detach clone, then attach) and leaves it healthy.
# Scenario:
# - create a packed table with a projection, capture SELECT baseline
# - DETACH PART; assert the detached copy carries p.proj/data.packed
# - ATTACH PART; assert the projection is healthy and SELECT matches baseline
def test_packed_detach_attach_carries_projection():
    setup_table("t_pk_da", PACKED)
    baseline = proj_query("t_pk_da")
    live = part_dir("t_pk_da")
    name = part_name("t_pk_da")
    table_root = live.rsplit("/", 1)[0]
    assert path_exists(f"{live}/data.packed")

    node.query(f"ALTER TABLE t_pk_da DETACH PART '{name}'")
    assert not path_exists(live)
    assert path_exists(f"{table_root}/detached/{name}/data.packed")
    assert path_exists(f"{table_root}/detached/{name}/p.proj/data.packed")

    node.query(f"ALTER TABLE t_pk_da ATTACH PART '{name}'")
    p = part_dir("t_pk_da")
    assert path_exists(f"{p}/p.proj/data.packed")
    assert broken_projection_parts("t_pk_da") == "0"
    assert (
        proj_query("t_pk_da", extra_settings="force_optimize_projection = 1") == baseline
    )
    assert check_table("t_pk_da") == "1"


# This test checks that a mutation of a projected column rebuilds the packed projection (checksums
# recalculated) and the rebuilt projection reflects the mutated data.
# Scenario:
# - create a packed table with a projection
# - ALTER UPDATE a column read by the projection
# - assert the projection is healthy, serves the updated data, and the part passes CHECK TABLE
def test_packed_mutation_rebuilds_projection():
    setup_table("t_pk_mut", PACKED)
    assert path_exists(f"{part_dir('t_pk_mut')}/data.packed")
    baseline = proj_query("t_pk_mut")

    # mutate a non-key column the projection materializes
    node.query(
        "ALTER TABLE t_pk_mut UPDATE value = concat(value, 'xx') WHERE id < 200 SETTINGS mutations_sync = 2"
    )

    p = part_dir("t_pk_mut")
    assert path_exists(f"{p}/p.proj/data.packed")
    assert broken_projection_parts("t_pk_mut") == "0"
    assert (
        proj_query("t_pk_mut", extra_settings="force_optimize_projection = 1")
        == baseline
    )
    # 100 rows have id < 200 (values "0".."99": 10 + 2*90 = 190 chars), each grew by 2 chars
    updated = node.query(
        "SELECT count(), sum(length(value)) FROM t_pk_mut WHERE id < 200 "
        "SETTINGS optimize_use_projections = 1, force_optimize_projection = 1"
    ).strip()
    assert updated == "100\t390"
    assert check_table("t_pk_mut") == "1"


# ==============================================================================
# N. Object-storage rebuild-path regression
#
# A projection REBUILT during a mutation on an object-storage disk must survive the part's final
# rename. Object-storage disks run DEFERRED part transactions (master #89658 / 228ca4c8a4c): the
# rebuilt projection's `<n>.tmp_proj -> <name>.proj` move is queued and not yet on disk when the
# mutation's finalize re-derives the part's owned projection set from a disk probe, so the probe
# misses it and the tmp->final repoint is skipped. The failure surfaces at READ time as
# `Code 107 FILE_DOESNT_EXIST` on a `tmp_mut_.../<name>.proj/...` path, after the mutation itself
# succeeded. On the default (local) disk the transaction is immediate, the probe sees the move, and
# the bug is invisible -- which is why every other test in this file, all on the local disk, misses
# it. Each test runs both on 'local' (control: must pass) and 's3' (the regression surface).
#
# Fail-closed: force_optimize_projection = 1 throws if the projection cannot serve the query, so a
# silently-skipped (unused) projection cannot masquerade as a pass -- that is exactly what let the
# regression through the existing suite.
# ==============================================================================


# This test checks the canonical failing class: ADD then MATERIALIZE a projection (a rebuild inside
# a mutation) on an object-storage disk. The materialize succeeds; the projection must then be
# readable, healthy, and survive a reload -- not left rooted at the vacated tmp_mut_ path.
def test_os_materialize_projection_rebuild(storage_kind, disk):
    node.query("DROP TABLE IF EXISTS t_os_mat SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        f"""CREATE TABLE t_os_mat (key UInt64, id UInt64, value String)
           ENGINE = MergeTree ORDER BY key
           SETTINGS {with_disk(with_storage("min_bytes_for_wide_part = 0", storage_kind), disk)}"""
    )
    node.query(
        "INSERT INTO t_os_mat SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # add and materialize the projection (rebuild path inside a mutation)
    node.query("ALTER TABLE t_os_mat ADD PROJECTION p (SELECT key, id, value ORDER BY id)")
    node.query("ALTER TABLE t_os_mat MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2")

    # the mutation succeeded; a projection read must not open a vacated tmp_mut_ path
    assert broken_projection_parts("t_os_mat") == "0"
    assert (
        proj_query("t_os_mat", extra_settings="force_optimize_projection = 1") == "100\t4950"
    )
    assert check_table("t_os_mat") == "1"

    # the projection also survives a reload from disk (the load path this fix relocates)
    node.restart_clickhouse()
    block_until_tables_loaded("t_os_mat")
    assert broken_projection_parts("t_os_mat") == "0"
    assert (
        proj_query("t_os_mat", extra_settings="force_optimize_projection = 1") == "100\t4950"
    )


# This test checks the same rebuild via a plain data mutation: ALTER UPDATE of a column the
# projection materializes forces the projection to be rebuilt inside the mutation.
def test_os_update_projected_column_rebuild(storage_kind, disk):
    node.query("DROP TABLE IF EXISTS t_os_upd SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        f"""CREATE TABLE t_os_upd (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS {with_disk(with_storage("min_bytes_for_wide_part = 0", storage_kind), disk)}"""
    )
    node.query(
        "INSERT INTO t_os_upd SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # UPDATE a projected column -> the mutation rebuilds the projection
    node.query(
        "ALTER TABLE t_os_upd UPDATE value = concat(value, '!') WHERE id < 200 SETTINGS mutations_sync = 2"
    )
    assert broken_projection_parts("t_os_upd") == "0"
    assert (
        proj_query("t_os_upd", extra_settings="force_optimize_projection = 1") == "100\t4950"
    )
    assert check_table("t_os_upd") == "1"

    # survives a reload from disk (load path)
    node.restart_clickhouse()
    block_until_tables_loaded("t_os_upd")
    assert broken_projection_parts("t_os_upd") == "0"
    assert check_table("t_os_upd") == "1"


# This test checks the lightweight-DELETE rebuild path (lightweight_mutation_projection_mode =
# 'rebuild'): the deleting mutation re-materializes the projection, which must survive on object storage.
def test_os_lightweight_delete_rebuild(storage_kind, disk):
    node.query("DROP TABLE IF EXISTS t_os_lwd SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        f"""CREATE TABLE t_os_lwd (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS {with_disk(with_storage("min_bytes_for_wide_part = 0, lightweight_mutation_projection_mode = 'rebuild'", storage_kind), disk)}"""
    )
    node.query(
        "INSERT INTO t_os_lwd SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # lightweight DELETE rebuilds the projection
    node.query("DELETE FROM t_os_lwd WHERE key >= 500 SETTINGS mutations_sync = 2")
    assert node.query("SELECT count() FROM t_os_lwd").strip() == "500"
    assert broken_projection_parts("t_os_lwd") == "0"
    # rows with id < 200 (key < 100) are untouched by the delete: still 100 rows, sum(key) = 4950
    assert (
        proj_query("t_os_lwd", extra_settings="force_optimize_projection = 1") == "100\t4950"
    )
    assert check_table("t_os_lwd") == "1"


# This test probes the merge path: two parts merged with OPTIMIZE FINAL rebuild the projection into
# the merged part, which must stay readable on object storage. (Merges use a different driver than
# mutations; this guards that path too.)
def test_os_optimize_final_merge(storage_kind, disk):
    node.query("DROP TABLE IF EXISTS t_os_merge SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        f"""CREATE TABLE t_os_merge (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS {with_disk(with_storage("min_bytes_for_wide_part = 0", storage_kind), disk)}"""
    )
    for rng in ("numbers(1000)", "numbers(1000, 1000)"):
        node.query(
            f"INSERT INTO t_os_merge SELECT number, number * 2, toString(number) FROM {rng}"
        )

    # merge into a single part; the merged part rebuilds the projection
    node.query("SYSTEM START MERGES t_os_merge")
    node.query("OPTIMIZE TABLE t_os_merge FINAL")
    wait_for(lambda: active_parts("t_os_merge") == "1")

    assert broken_projection_parts("t_os_merge") == "0"
    assert (
        proj_query("t_os_merge", extra_settings="force_optimize_projection = 1") == "100\t4950"
    )
    assert check_table("t_os_merge") == "1"


# This test is the CONTROL: a mutation of a column the projection does NOT reference carries the
# projection by hardlink/copy rather than rebuilding it. The carry loop commits its own part
# transaction early, so this must pass both before and after the fix -- it proves the regression is
# specific to the REBUILD path, not to object-storage mutations in general.
def test_os_mutation_carries_unprojected_column(storage_kind, disk):
    node.query("DROP TABLE IF EXISTS t_os_carry SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        f"""CREATE TABLE t_os_carry (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS {with_disk(with_storage("min_bytes_for_wide_part = 0", storage_kind), disk)}"""
    )
    node.query(
        "INSERT INTO t_os_carry SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # UPDATE a column the projection does not reference -> projection is carried, not rebuilt
    node.query(
        "ALTER TABLE t_os_carry UPDATE value = concat(value, '!') WHERE id < 200 SETTINGS mutations_sync = 2"
    )
    assert broken_projection_parts("t_os_carry") == "0"
    assert (
        proj_query("t_os_carry", extra_settings="force_optimize_projection = 1") == "100\t4950"
    )
    assert check_table("t_os_carry") == "1"
