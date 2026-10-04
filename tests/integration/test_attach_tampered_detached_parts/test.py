# Detached part directories manipulated on disk before ATTACH / DROP DETACHED:
# fabricated `_tryN` leftovers, injected canned old-version parts, and legacy
# index file renames (`.idx` vs `.idx2`, `checksums.txt` removal). These tests
# emulate parts written by OLD ClickHouse versions, so they tamper with the
# server's on-disk data and therefore live here rather than in stateless tests.
#
# Converted from the stateless tests:
#   04063_drop_detached_part_with_try_n_suffix.sh
#   04246_materialize_index_force_recalc.sh
#   04402_mutate_all_columns_preserve_legacy_idx_minmax.sh
#   04403_mutate_preserve_legacy_idx_packed_minmax.sh
#   04404_mutate_rebuild_legacy_idx_minmax.sh
#   04425_mutate_mixed_legacy_idx_minmax.sh
#
# The originals carried `no-object-storage` / `no-shared-merge-tree` /
# `no-replicated-database` / `no-random-merge-tree-settings` tags because the
# fixtures edit real local part files and rely on ATTACH recomputing
# `checksums.txt` from them; a single plain local-disk node satisfies all of
# that by construction.

import os

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def exec_root(cmd):
    return node.exec_in_container(["bash", "-c", cmd], privileged=True, user="root")


def ls_entry_exists(path):
    # Mirrors the originals' `if ls <path> >/dev/null 2>&1; then echo 1; else echo 0; fi`.
    return exec_root(f"if ls {path} >/dev/null 2>&1; then echo 1; else echo 0; fi").strip()


def table_data_path(table):
    return node.query(
        f"SELECT data_paths[1] FROM system.tables WHERE database = 'default' AND table = '{table}'"
    ).strip()


def find_detached_part_dir(data_path):
    part_dir = exec_root(
        f"find {data_path}detached -maxdepth 1 -type d -name 'all_*' | head -1"
    ).strip()
    assert part_dir, "no detached part directory found"
    return part_dir


def active_part_dir(table):
    # Resolve the active part directory via `system.parts` (a mutation leaves the
    # old parts inactive but still on disk, so a plain directory scan would pick
    # the wrong one).
    return node.query(
        f"SELECT path FROM system.parts WHERE database = 'default' AND table = '{table}' AND active ORDER BY name LIMIT 1"
    ).strip().rstrip("/")


def prunes_to_one_granule(table, column):
    # The indexed column equals k (monotone), so the minmax index prunes a point
    # query to a single granule: `EXPLAIN indexes = 1` must show "Granules: 1/20".
    return node.query(
        f"SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM {table} WHERE {column} = 42) WHERE explain ILIKE '%Granules: 1/20%'"
    )


def test_drop_detached_part_with_try_n_suffix(started_cluster):
    # Converted from stateless test 04063_drop_detached_part_with_try_n_suffix.sh.
    #
    # Regression test for parsing detached part names with the _tryN suffix.
    # The bug caused BAD_DATA_PART_NAME when trying to drop such parts with zero-copy
    # replication, and made DROP DETACHED PARTITION ALL skip them silently everywhere
    # (the unparsable name excluded them from the partition-level drop).
    #
    # The leftover "_tryN" directories are produced here with a filesystem-level "cp -r" of the
    # original detached part directory. On object storage this only copies the local metadata, so
    # all copies alias the same remote blobs; dropping the copies then removes blobs the original
    # still references, and the subsequent "ATTACH PARTITION ALL" fails because the data is gone.
    # The bug under test (detached part name parsing) is storage-agnostic, so the test runs on
    # local disk only; the table is pinned to the built-in local "default" storage policy.
    table = "t_drop_detached_try_n"
    node.query(f"DROP TABLE IF EXISTS {table}")
    node.query(
        f"""
        CREATE TABLE {table} (n UInt64)
        ENGINE = MergeTree ORDER BY n
        SETTINGS storage_policy = 'default'
        """
    )

    node.query(f"INSERT INTO {table} VALUES (1), (42)")

    # Determine the actual part name. The first allocated block number is not
    # necessarily 0 (e.g. under a Replicated database it comes from a Keeper
    # counter), so the name must not be hardcoded.
    part = node.query(
        f"SELECT name FROM system.parts WHERE table = '{table}' AND database = 'default' AND active LIMIT 1"
    ).strip()

    # Detach the part
    node.query(f"ALTER TABLE {table} DETACH PART '{part}'")

    # Get the path to the detached directory (parent of the part directory)
    part_path = node.query(
        f"SELECT path FROM system.detached_parts WHERE table = '{table}' AND database = 'default' LIMIT 1"
    ).strip()
    detached_dir = os.path.dirname(part_path.rstrip("/"))

    # Create leftover copies of the detached part covering the suffix variants:
    #   *_try1                       - single digit
    #   covered-by-broken_*_try1     - with a known prefix
    #   *_try100                     - multiple digits (must be accepted)
    #   *_try                        - no digits (must NOT be treated as a tryN suffix)
    for name in (
        f"{part}_try1",
        f"covered-by-broken_{part}_try1",
        f"{part}_try100",
        f"{part}_try",
    ):
        exec_root(f"cp -r {detached_dir}/{part} {detached_dir}/{name}")

    def detached_names():
        # Part name normalized for stable comparison, as the original's `sed`.
        return node.query(
            f"SELECT name FROM system.detached_parts WHERE table = '{table}' AND database = 'default' ORDER BY name"
        ).replace(part, "PART")

    # List detached parts - should see all of them
    assert detached_names() == "PART\nPART_try\nPART_try1\nPART_try100\ncovered-by-broken_PART_try1\n"

    # Drop the detached parts with _tryN suffix - this used to fail with BAD_DATA_PART_NAME
    node.query(
        f"ALTER TABLE {table} DROP DETACHED PART 'covered-by-broken_{part}_try1' SETTINGS allow_drop_detached = 1"
    )
    node.query(
        f"ALTER TABLE {table} DROP DETACHED PART '{part}_try1' SETTINGS allow_drop_detached = 1"
    )
    # A multi-digit suffix must also be droppable
    node.query(
        f"ALTER TABLE {table} DROP DETACHED PART '{part}_try100' SETTINGS allow_drop_detached = 1"
    )

    # The original part and the malformed "_try" directory (not a tryN suffix) should remain
    assert detached_names() == "PART\nPART_try\n"

    # ATTACH PARTITION ALL must not be broken by a leftover "_tryN" directory: it should
    # attach the original part and silently ignore the suffixed leftovers.
    node.query(f"ALTER TABLE {table} ATTACH PARTITION ALL")
    assert node.query(f"SELECT n FROM {table} ORDER BY n") == "1\n42\n"

    # The malformed "_try" directory is not a valid attach candidate, so it stays detached
    assert detached_names() == "PART_try\n"

    # DROP DETACHED PARTITION ALL must drop "_tryN" leftovers too. Before the fix they were
    # skipped silently, because their directory name failed to parse (valid_name was false),
    # so a partition-level drop could never remove them.
    part2 = node.query(
        f"SELECT name FROM system.parts WHERE table = '{table}' AND database = 'default' AND active LIMIT 1"
    ).strip()
    node.query(f"ALTER TABLE {table} DETACH PART '{part2}'")
    exec_root(f"cp -r {detached_dir}/{part2} {detached_dir}/{part2}_try7")

    node.query(f"ALTER TABLE {table} DROP DETACHED PARTITION ALL SETTINGS allow_drop_detached = 1")

    # Everything except the malformed "_try" directory (not a tryN suffix) must be gone
    result = node.query(
        f"SELECT name FROM system.detached_parts WHERE table = '{table}' AND database = 'default' ORDER BY name"
    ).replace(part2, "PART2").replace(part, "PART")
    assert result == "PART_try\n"

    node.query(f"DROP TABLE {table} SYNC")


def test_materialize_index_force_recalc(started_cluster):
    # Converted from stateless test 04246_materialize_index_force_recalc.sh.
    #
    # Regression test for ClickHouse/ClickHouse#104872.
    #
    # Background: PR #91980 widened the force-recalculate predicate in
    # `MutateFromLogEntryTask::prepare` from `!is_full_part_storage` to
    # `!is_full_wide_part`, which made Compact parts force-recalculate every
    # pre-existing skip index on every mutation. The follow-up
    # `splitAndModifyMutationCommands` only added columns of the
    # *explicitly-materialized* index to the read set. As a result a pre-existing
    # index over a column that is in the table metadata but absent from the part
    # on disk crashed the next mutation on that part with
    # `NOT_FOUND_COLUMN_IN_BLOCK`.
    #
    # The broken on-disk shape was produced by 25.8: in 25.8 `MATERIALIZE INDEX`
    # wrote `skp_idx_<NAME>.idx` for the new index but did *not* add the indexed
    # column to the new part's `columns.txt`. After an upgrade to 25.10+ /
    # 26.3+ / master, any subsequent mutation on the part hit the bug.
    #
    # We cannot reproduce the bug by creating a fresh table on a modern build —
    # the modern `MATERIALIZE INDEX` correctly writes the index's columns to the
    # new part, so the broken shape never appears. Therefore this test attaches a
    # pre-captured 25.8-era Compact part (`part_25.8.tar.gz`, 1.2 KiB) that has
    # the broken shape on disk:
    #
    #   columns.txt: 2 columns: `timestamp`, `requestID`   (no `vid`)
    #   skp_idx_vid_ix.idx, skp_idx_vid_ix.cmrk4           (present)
    #
    # After attaching, we replay the user's reproducer (`ADD INDEX requestID_ix`
    # + `MATERIALIZE INDEX requestID_ix`). On master without the fix this
    # mutation force-recalculates `vid_ix` while the read set is missing `vid`
    # and we get `NOT_FOUND_COLUMN_IN_BLOCK`. With the fix the read set covers
    # every pre-existing index's columns and the mutation succeeds.
    part_name = "all_1_1_0_2"

    node.query("DROP TABLE IF EXISTS issue_104872 SYNC")

    # The schema matches the table state after `ADD COLUMN vid` + `ADD INDEX vid_ix`
    # in the user's 25.8 session. The captured part was created in that exact
    # session — its on-disk `columns.txt` is from before `ADD COLUMN vid` because
    # 25.8's `MATERIALIZE INDEX vid_ix` did not yet add the column to the new
    # part. That is the broken shape this test exercises.
    node.query(
        """
        CREATE TABLE issue_104872
        (
            timestamp DateTime,
            requestID String,
            vid Int64,
            INDEX vid_ix vid TYPE bloom_filter GRANULARITY 100
        )
        ENGINE = MergeTree() ORDER BY timestamp
        SETTINGS index_granularity = 8192, min_bytes_for_wide_part = '10G'
        """
    )

    # Locate the table's data directory and extract the captured 25.8 part into
    # the `detached/` subdirectory, then attach it.
    data_path = table_data_path("issue_104872")

    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "part_25.8.tar.gz"), "/part_25.8.tar.gz"
    )
    exec_root(
        f"mkdir -p {data_path}detached/{part_name} && tar -xzf /part_25.8.tar.gz -C {data_path}detached/{part_name}"
    )

    node.query(f"ALTER TABLE issue_104872 ATTACH PART '{part_name}'")

    # Sanity check: the part is Compact, has the broken shape (column `vid`
    # missing on disk), and the table reads correctly with `vid` defaulted to 0.
    assert (
        node.query(
            "SELECT part_type, name FROM system.parts WHERE database = 'default' AND table = 'issue_104872' AND active ORDER BY name"
        )
        == "Compact\tall_1_1_0\n"
    )
    assert node.query("SELECT requestID, vid FROM issue_104872 ORDER BY requestID") == "aaa\t0\nbbb\t0\n"

    # The bug: adding a second skip index and materializing it force-recalculates
    # the pre-existing `vid_ix` whose required column `vid` is not in the part on
    # disk. Without the fix, the mutation fails with NOT_FOUND_COLUMN_IN_BLOCK.
    node.query("ALTER TABLE issue_104872 ADD INDEX requestID_ix requestID TYPE bloom_filter GRANULARITY 100")
    node.query("ALTER TABLE issue_104872 MATERIALIZE INDEX requestID_ix SETTINGS mutations_sync = 2")

    # Verify the mutation succeeded and the data is intact.
    assert node.query("SELECT count() FROM issue_104872") == "2\n"
    assert node.query("SELECT requestID, vid FROM issue_104872 ORDER BY requestID") == "aaa\t0\nbbb\t0\n"

    # A subsequent DELETE also force-recalculates the pre-existing indices on the
    # Compact part — covers the non-`MATERIALIZE INDEX` mutation entry points.
    node.query("ALTER TABLE issue_104872 DELETE WHERE requestID = 'aaa' SETTINGS mutations_sync = 2")
    assert node.query("SELECT count() FROM issue_104872") == "1\n"
    assert node.query("SELECT requestID, vid FROM issue_104872 ORDER BY requestID") == "bbb\t0\n"

    node.query("DROP TABLE issue_104872 SYNC")


def test_mutate_all_columns_preserve_legacy_idx_minmax(started_cluster):
    # Converted from stateless test 04402_mutate_all_columns_preserve_legacy_idx_minmax.sh.
    #
    # A full-part rewrite must keep a non-recalculated minmax index whose data file is still the
    # legacy `.idx` (v1) rather than the current `.idx2` (v2); it used to hardlink the mark but not
    # that data file, silently dropping the index. Issue #109595.
    #
    # The modern writer only emits `.idx2`, so the legacy shape is fabricated: DETACH, rename
    # `.idx2` to `.idx` (byte-identical payloads for a non-nullable column), drop `checksums.txt`
    # so ATTACH recomputes it, ATTACH, then mutate.
    node.query("DROP TABLE IF EXISTS t_legacy_minmax SYNC")

    # v = k is monotone, so the minmax index over v prunes a point query to a
    # single granule. `index_granularity` = 100 over 2000 rows gives 20 granules.
    #
    # The settings the file surgery below depends on are pinned in the CREATE:
    # index_granularity fixes the granule count (2000 rows / 100 = 20 granules),
    # replace_long_file_name_to_hash = 0 keeps the index file at its logical name
    # (skp_idx_mm_v.idx2) that we rename, min_bytes_for_wide_part = 0 forces the
    # Wide layout, and packed_skip_index_max_bytes = 0 keeps that file standalone
    # rather than a member of skp_idx.packed, which the rename cannot reach.
    node.query(
        """
        CREATE TABLE t_legacy_minmax
        (
            k UInt64,
            v UInt64,
            m Map(String, UInt64) MATERIALIZED map('a', k),
            INDEX mm_v v TYPE minmax GRANULARITY 1
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 index_granularity = 100, replace_long_file_name_to_hash = 0,
                 packed_skip_index_max_bytes = 0,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """
    )

    node.query("INSERT INTO t_legacy_minmax (k, v) SELECT number, number FROM numbers(2000)")
    node.query("OPTIMIZE TABLE t_legacy_minmax FINAL")

    # Detach so we can rewrite the part files, then downgrade the minmax index to
    # the legacy ".idx" layout.
    node.query("ALTER TABLE t_legacy_minmax DETACH PARTITION tuple() SETTINGS mutations_sync = 2")

    data_path = table_data_path("t_legacy_minmax")
    part_dir = find_detached_part_dir(data_path)

    exec_root(f"mv {part_dir}/skp_idx_mm_v.idx2 {part_dir}/skp_idx_mm_v.idx")
    exec_root(f"rm -f {part_dir}/checksums.txt")

    node.query("ALTER TABLE t_legacy_minmax ATTACH PARTITION tuple()")

    # Sanity: the legacy ".idx" index is recognized and prunes to one granule.
    assert prunes_to_one_granule("t_legacy_minmax", "v") == "1\n"

    # Full part rewrite: `DROP COLUMN` of a MATERIALIZED column takes
    # `MutateAllPartColumnsTask`. The minmax index is not recalculated, so it is
    # hardlinked from the source part. Before the fix the legacy ".idx" data file
    # was dropped here and the index no longer pruned (Granules: 20/20).
    node.query("ALTER TABLE t_legacy_minmax DROP COLUMN m SETTINGS mutations_sync = 2")

    assert (
        node.query("CHECK TABLE t_legacy_minmax SETTINGS check_query_single_value_result = 0")
        == "all_2_2_0_3\t1\t\n"
    )

    # The preserved legacy index must still prune to one granule (was 20/20 before the fix).
    assert prunes_to_one_granule("t_legacy_minmax", "v") == "1\n"
    assert node.query("SELECT count() FROM t_legacy_minmax WHERE v = 42") == "1\n"

    # The on-disk size accounting must include the preserved legacy ".idx" payload.
    # `calculateSecondaryIndicesSizesOnDisk` used to enumerate `getSubstreams` (only
    # ".idx2"), so on the repaired part it counted the mark file but missed the ".idx"
    # data file and reported `secondary_indices_compressed_bytes` = 0. The fix probes the
    # substreams actually present via `getAllSubstreamsInPart`.
    assert (
        node.query(
            "SELECT secondary_indices_compressed_bytes > 0 FROM system.parts WHERE database = 'default' AND table = 't_legacy_minmax' AND active"
        )
        == "1\n"
    )

    node.query("DROP TABLE t_legacy_minmax SYNC")


def test_mutate_preserve_legacy_idx_packed_minmax(started_cluster):
    # Converted from stateless test 04403_mutate_preserve_legacy_idx_packed_minmax.sh.
    #
    # Packed-archive counterpart of 04402: rebuilding `skp_idx.packed` for a recomputed index (mm_w)
    # must preload the surviving members of the preserved index (mm_v), including a legacy `.idx`
    # data member. It used to preload only the mark, dropping mm_v's data. Issue #109595.
    #
    # Legacy shape fabricated by rewriting the packed footer to rename mm_v's `.idx2` member to
    # `.idx` (byte-identical payloads for a non-nullable column), then ATTACH and `ALTER UPDATE` w.
    node.query("DROP TABLE IF EXISTS t_legacy_packed SYNC")

    # v = k and w = k are monotone, so each minmax index prunes a point query to a
    # single granule. `index_granularity` = 100 over 2000 rows gives 20 granules.
    node.query(
        """
        CREATE TABLE t_legacy_packed
        (
            k UInt64,
            v UInt64,
            w UInt64,
            INDEX mm_v v TYPE minmax GRANULARITY 1,
            INDEX mm_w w TYPE minmax GRANULARITY 1
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 index_granularity = 100, replace_long_file_name_to_hash = 0,
                 packed_skip_index_max_bytes = 1000000,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """
    )

    node.query("INSERT INTO t_legacy_packed (k, v, w) SELECT number, number, number FROM numbers(2000)")
    node.query("OPTIMIZE TABLE t_legacy_packed FINAL")

    # Detach so we can rewrite the packed archive, then downgrade mm_v to the legacy
    # ".idx" layout inside skp_idx.packed.
    node.query("ALTER TABLE t_legacy_packed DETACH PARTITION tuple() SETTINGS mutations_sync = 2")

    data_path = table_data_path("t_legacy_packed")
    part_dir = find_detached_part_dir(data_path)

    exec_root(f"chmod u+w {part_dir}/skp_idx.packed")
    # Rewrite the packed footer, renaming skp_idx_mm_v.idx2 -> skp_idx_mm_v.idx
    # (see rewrite_packed_footer.py; it fails loudly if the member is missing,
    # so the fixture cannot silently degrade into a no-op).
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "rewrite_packed_footer.py"), "/rewrite_packed_footer.py"
    )
    exec_root(
        f"python3 /rewrite_packed_footer.py {part_dir}/skp_idx.packed skp_idx_mm_v.idx2 skp_idx_mm_v.idx"
    )
    exec_root(f"rm -f {part_dir}/checksums.txt")

    node.query("ALTER TABLE t_legacy_packed ATTACH PARTITION tuple()")

    # Sanity: the legacy ".idx" minmax member inside the archive is recognized and
    # prunes to one granule.
    assert prunes_to_one_granule("t_legacy_packed", "v") == "1\n"

    # Rebuild the archive: `ALTER UPDATE` touches only w, so mm_w is recomputed (and the
    # packed archive is rewritten) while mm_v is preserved. Before the fix the legacy
    # ".idx" data member of mm_v was dropped from the new archive and no longer pruned.
    node.query("ALTER TABLE t_legacy_packed UPDATE w = w + 0 WHERE 1 SETTINGS mutations_sync = 2")

    assert node.query("CHECK TABLE t_legacy_packed SETTINGS check_query_single_value_result = 1") == "1\n"

    # The preserved legacy mm_v index must still prune to one granule (was 20/20 before
    # the fix), and the recomputed mm_w must prune too.
    assert prunes_to_one_granule("t_legacy_packed", "v") == "1\n"
    assert prunes_to_one_granule("t_legacy_packed", "w") == "1\n"
    assert node.query("SELECT count() FROM t_legacy_packed WHERE v = 42") == "1\n"

    node.query("DROP TABLE t_legacy_packed SYNC")


def test_mutate_rebuild_legacy_idx_minmax(started_cluster):
    # Converted from stateless test 04404_mutate_rebuild_legacy_idx_minmax.sh.
    #
    # Rebuild-path counterpart of 04402: when a mutation recomputes the index and writes a fresh
    # `.idx2`, the legacy `.idx` must be neither hardlinked forward nor left in the inherited
    # checksums. It used to leak dead beside the fresh file with a stale checksum. Issue #109595.
    #
    # Legacy shape fabricated as in 04402, then the indexed column is updated to force the rebuild.
    node.query("DROP TABLE IF EXISTS t_rebuild_minmax SYNC")

    # v = k is monotone, so the minmax index over v prunes a point query to a
    # single granule. `index_granularity` = 100 over 2000 rows gives 20 granules.
    node.query(
        """
        CREATE TABLE t_rebuild_minmax
        (
            k UInt64,
            v UInt64,
            INDEX mm_v v TYPE minmax GRANULARITY 1
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 index_granularity = 100, replace_long_file_name_to_hash = 0,
                 packed_skip_index_max_bytes = 0,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """
    )

    node.query("INSERT INTO t_rebuild_minmax (k, v) SELECT number, number FROM numbers(2000)")
    node.query("OPTIMIZE TABLE t_rebuild_minmax FINAL")

    # Detach so we can rewrite the part files, then downgrade the minmax index to
    # the legacy ".idx" layout.
    node.query("ALTER TABLE t_rebuild_minmax DETACH PARTITION tuple() SETTINGS mutations_sync = 2")

    data_path = table_data_path("t_rebuild_minmax")
    part_dir = find_detached_part_dir(data_path)

    exec_root(f"mv {part_dir}/skp_idx_mm_v.idx2 {part_dir}/skp_idx_mm_v.idx")
    exec_root(f"rm -f {part_dir}/checksums.txt")

    node.query("ALTER TABLE t_rebuild_minmax ATTACH PARTITION tuple()")

    # Sanity: the legacy ".idx" index is recognized and prunes to one granule.
    assert prunes_to_one_granule("t_rebuild_minmax", "v") == "1\n"

    # Rebuild mutation: `ALTER UPDATE` of the indexed column v recomputes mm_v. The
    # writer produces a fresh ".idx2". Before the fix the legacy ".idx" was
    # hardlinked into the new part (leaked dead) with a stale checksum entry.
    node.query("ALTER TABLE t_rebuild_minmax UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2")

    # The new ACTIVE part must carry the freshly written ".idx2" and NOT the stale
    # legacy ".idx": the rebuild path must strip/skip it, not hardlink it.
    new_part_dir = active_part_dir("t_rebuild_minmax")
    assert ls_entry_exists(f"{new_part_dir}/skp_idx_mm_v.idx") == "0"  # legacy_idx_leaked
    assert ls_entry_exists(f"{new_part_dir}/skp_idx_mm_v.idx2") == "1"  # current_idx2_present

    assert node.query("CHECK TABLE t_rebuild_minmax SETTINGS check_query_single_value_result = 1") == "1\n"

    # The recomputed index must still prune to one granule.
    assert prunes_to_one_granule("t_rebuild_minmax", "v") == "1\n"
    assert node.query("SELECT count() FROM t_rebuild_minmax WHERE v = 42") == "1\n"

    node.query("DROP TABLE t_rebuild_minmax SYNC")


def test_mutate_mixed_legacy_idx_minmax(started_cluster):
    # Converted from stateless test 04425_mutate_mixed_legacy_idx_minmax.sh.
    #
    # Mixed-format part: one index carrying BOTH a legacy `.idx` and a fresh `.idx2`, as an
    # intermediate buggy build could leave it. Cleanup keyed on the preferred read layout never saw
    # the stale `.idx`, so it was hardlinked forward with a stale checksum and the part stayed mixed
    # forever. Issue #109595.
    node.query("DROP TABLE IF EXISTS t_mixed_minmax SYNC")

    # v = k is monotone, so the minmax index over v prunes a point query to a single
    # granule. `index_granularity` = 100 over 2000 rows gives 20 granules.
    node.query(
        """
        CREATE TABLE t_mixed_minmax
        (
            k UInt64,
            v UInt64,
            INDEX mm_v v TYPE minmax GRANULARITY 1
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 index_granularity = 100, replace_long_file_name_to_hash = 0,
                 packed_skip_index_max_bytes = 0,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """
    )

    node.query("INSERT INTO t_mixed_minmax (k, v) SELECT number, number FROM numbers(2000)")
    node.query("OPTIMIZE TABLE t_mixed_minmax FINAL")

    # Detach so we can rewrite the part files, then fabricate the MIXED layout: keep
    # the modern ".idx2" AND add a stale legacy ".idx" copy (for a non-nullable
    # column the v1 and v2 minmax payloads are byte-identical), then drop checksums
    # so ATTACH recomputes them to include both files.
    node.query("ALTER TABLE t_mixed_minmax DETACH PARTITION tuple() SETTINGS mutations_sync = 2")

    data_path = table_data_path("t_mixed_minmax")
    part_dir = find_detached_part_dir(data_path)

    exec_root(f"cp {part_dir}/skp_idx_mm_v.idx2 {part_dir}/skp_idx_mm_v.idx")
    exec_root(f"rm -f {part_dir}/checksums.txt")

    node.query("ALTER TABLE t_mixed_minmax ATTACH PARTITION tuple()")

    # Sanity: the part carries BOTH files, and the index still prunes to one granule
    # (the reader prefers ".idx2").
    assert prunes_to_one_granule("t_mixed_minmax", "v") == "1\n"

    # Pre-cleanup size accounting: the mixed part carries BOTH data files, so the
    # reported index size must sum both (union walk), not only the preferred ".idx2".
    # The shared ".cmrk2" mark file is counted exactly once.
    #
    # Read this from `system.data_skipping_indices`, not from `system.parts`: the
    # `system.parts` secondary-index columns are served by the part-lifetime
    # `total_secondary_indices_size` accumulator, which is never reset, so an ATTACHed
    # part reports every size doubled. That is independent of this test (it reproduces
    # on unmodified master for a plain single-".idx2" part) and is tracked separately.
    mixed_part = active_part_dir("t_mixed_minmax")
    disk_data = int(exec_root(f"stat -c%s {mixed_part}/skp_idx_mm_v.idx").strip()) + int(
        exec_root(f"stat -c%s {mixed_part}/skp_idx_mm_v.idx2").strip()
    )
    mrk_file = exec_root(f"find {mixed_part} -maxdepth 1 -name 'skp_idx_mm_v.*mrk*' | head -1").strip()
    assert mrk_file, "no mark file found for the mm_v index"
    disk_marks = int(exec_root(f"stat -c%s {mrk_file}").strip())
    # size_counts_both_idx_payloads
    assert (
        node.query(
            f"SELECT data_compressed_bytes = {disk_data} FROM system.data_skipping_indices WHERE database = 'default' AND table = 't_mixed_minmax' AND name = 'mm_v'"
        )
        == "1\n"
    )
    # size_counts_marks_once
    assert (
        node.query(
            f"SELECT marks_bytes = {disk_marks} FROM system.data_skipping_indices WHERE database = 'default' AND table = 't_mixed_minmax' AND name = 'mm_v'"
        )
        == "1\n"
    )

    # --- Case 1: rebuild mutation (`ALTER UPDATE` of the indexed column) ---
    # The writer produces a fresh ".idx2". The stale ".idx" must be stripped, not
    # hardlinked forward.
    node.query("ALTER TABLE t_mixed_minmax UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2")

    new_part_dir = active_part_dir("t_mixed_minmax")
    assert ls_entry_exists(f"{new_part_dir}/skp_idx_mm_v.idx") == "0"  # rebuild_stale_idx_leaked
    assert ls_entry_exists(f"{new_part_dir}/skp_idx_mm_v.idx2") == "1"  # rebuild_idx2_present
    assert node.query("CHECK TABLE t_mixed_minmax SETTINGS check_query_single_value_result = 1") == "1\n"
    # rebuild_prunes
    assert prunes_to_one_granule("t_mixed_minmax", "v") == "1\n"

    node.query("DROP TABLE t_mixed_minmax SYNC")

    # --- Case 2: `DROP INDEX` on a mixed part (fresh table) ---
    # `DROP INDEX` has no mutation pipeline; the stale ".idx" must be removed, not
    # hardlinked into the new part. Use a fresh table so the fabrication starts
    # from a clean (writable) source part, not a mutated one.
    node.query("DROP TABLE IF EXISTS t_mixed_minmax_drop SYNC")
    node.query(
        """
        CREATE TABLE t_mixed_minmax_drop
        (
            k UInt64,
            v UInt64,
            INDEX mm_v v TYPE minmax GRANULARITY 1
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 index_granularity = 100, replace_long_file_name_to_hash = 0,
                 packed_skip_index_max_bytes = 0,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """
    )
    node.query("INSERT INTO t_mixed_minmax_drop (k, v) SELECT number, number FROM numbers(2000)")
    node.query("OPTIMIZE TABLE t_mixed_minmax_drop FINAL")
    node.query("ALTER TABLE t_mixed_minmax_drop DETACH PARTITION tuple() SETTINGS mutations_sync = 2")

    data_path2 = table_data_path("t_mixed_minmax_drop")
    part_dir2 = find_detached_part_dir(data_path2)
    exec_root(f"chmod u+w {part_dir2}")
    exec_root(f"cp {part_dir2}/skp_idx_mm_v.idx2 {part_dir2}/skp_idx_mm_v.idx")
    exec_root(f"rm -f {part_dir2}/checksums.txt")
    node.query("ALTER TABLE t_mixed_minmax_drop ATTACH PARTITION tuple()")

    node.query("ALTER TABLE t_mixed_minmax_drop DROP INDEX mm_v SETTINGS mutations_sync = 2, alter_sync = 2")
    drop_part_dir = active_part_dir("t_mixed_minmax_drop")
    assert ls_entry_exists(f"{drop_part_dir}/skp_idx_mm_v.idx") == "0"  # drop_stale_idx_leaked
    assert ls_entry_exists(f"{drop_part_dir}/skp_idx_mm_v.idx2") == "0"  # drop_idx2_leaked
    assert node.query("CHECK TABLE t_mixed_minmax_drop SETTINGS check_query_single_value_result = 1") == "1\n"

    node.query("DROP TABLE t_mixed_minmax_drop SYNC")
