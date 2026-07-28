#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
#
# `no-fasttest`: this test does local-disk part-file surgery (renames the
# on-disk index file in the detached part dir) like other fixture-surgery
# tests (e.g. 02864_restore_table_with_broken_part). The Fast test macOS
# (arm_darwin) environment does not reliably expose that layout, so the rename
# cannot run there. The bug is disk-layer independent and is fully covered by
# the sanitizer stateless jobs.
#
# `no-object-storage` / `no-shared-merge-tree` / `no-replicated-database`: this
# test renames real on-disk index files in the local part directory and relies
# on ATTACH recomputing `checksums.txt` from those files. On object storage the
# files in the data dir are DiskObjectStorageMetadata pointer files, and the
# replicated/shared engines gate ATTACH on ZooKeeper checksum digests, so the
# local-disk file surgery below does not apply there. The bug is in
# `MutateAllPartColumnsTask`'s index-preservation loop and is independent of the
# disk layer, so a plain local MergeTree is sufficient.
#
# `no-random-merge-tree-settings`: the test renames a specific standalone
# index file (skp_idx_mm_v.idx2) and depends on a fixed granule count. Randomized
# merge-tree settings (`packed_skip_index_max_bytes` packs the index into
# skp_idx.packed, index_granularity_bytes / adaptive granularity change the
# granule count) would break the file surgery. The settings the test relies on
# are pinned explicitly in the CREATE below for the same reason.
#
# Regression test for the backward-compatibility gap flagged on PR #109616
# (issue #109595). A full-part-rewrite mutation (`MutateAllPartColumnsTask`)
# hardlinks non-recalculated skip indices from the source part. The loop used
# to enumerate the index's current writer substreams via `getSubstreams`. For
# minmax the on-disk format changed from ".idx" (v1) to ".idx2" (v2), so
# `getSubstreams` reports only ".idx2". On an upgraded part that still carries
# a legacy "skp_idx_<name>.idx" file the loop hardlinked the mark file but
# never found the ".idx" data file, silently dropping the index after the
# mutation (`CHECK TABLE` still passed because the orphan mark got checksummed).
# The fix enumerates the substreams actually present in the source part via
# getAllSubstreamsInPart(source_part->checksums, ...), which probes both ".idx"
# and ".idx2".
#
# The modern writer only produces ".idx2", so the legacy shape is fabricated:
# build a normal part, DETACH it, rename ".idx2" to ".idx" (for a non-nullable
# column the v1 and v2 minmax payloads are byte-identical), drop `checksums.txt`
# so ATTACH recomputes it, then ATTACH and run the full-rewrite mutation.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_legacy_minmax SYNC"

# v = k is monotone, so the minmax index over v prunes a point query to a
# single granule. `index_granularity` = 100 over 2000 rows gives 20 granules.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_legacy_minmax
(
    k UInt64,
    v UInt64,
    m Map(String, UInt64) MATERIALIZED map('a', k),
    INDEX mm_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
-- Pin the settings the file surgery below depends on so CI's randomized
-- merge-tree settings cannot break it: index_granularity fixes the granule
-- count (2000 rows / 100 = 20 granules), replace_long_file_name_to_hash = 0
-- keeps the index file at its logical name (skp_idx_mm_v.idx2) that we rename,
-- and min_bytes_for_wide_part = 0 forces the Wide layout.
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 100, replace_long_file_name_to_hash = 0,
         columns_and_secondary_indices_sizes_lazy_calculation = 0"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_legacy_minmax (k, v) SELECT number, number FROM numbers(2000)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_legacy_minmax FINAL"

# Detach so we can rewrite the part files, then downgrade the minmax index to
# the legacy ".idx" layout.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_legacy_minmax DETACH PARTITION tuple() SETTINGS mutations_sync = 2"

DATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 't_legacy_minmax'")
PART_DIR=$(find "${DATA_PATH}detached" -maxdepth 1 -type d -name 'all_*' | head -1)

mv "${PART_DIR}/skp_idx_mm_v.idx2" "${PART_DIR}/skp_idx_mm_v.idx"
rm -f "${PART_DIR}/checksums.txt"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_legacy_minmax ATTACH PARTITION tuple()"

# Sanity: the legacy ".idx" index is recognized and prunes to one granule.
echo "before:"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_legacy_minmax WHERE v = 42) WHERE explain ILIKE '%Granules: 1/20%'"

# Full part rewrite: DROP COLUMN of a MATERIALIZED column takes
# `MutateAllPartColumnsTask`. The minmax index is not recalculated, so it is
# hardlinked from the source part. Before the fix the legacy ".idx" data file
# was dropped here and the index no longer pruned (Granules: 20/20).
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_legacy_minmax DROP COLUMN m SETTINGS mutations_sync = 2"

${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_legacy_minmax SETTINGS check_query_single_value_result = 0"

# The preserved legacy index must still prune to one granule (was 20/20 before the fix).
echo "after:"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_legacy_minmax WHERE v = 42) WHERE explain ILIKE '%Granules: 1/20%'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_legacy_minmax WHERE v = 42"

# The on-disk size accounting must include the preserved legacy ".idx" payload.
# `calculateSecondaryIndicesSizesOnDisk` used to enumerate `getSubstreams` (only
# ".idx2"), so on the repaired part it counted the mark file but missed the ".idx"
# data file and reported secondary_indices_compressed_bytes = 0. The fix probes the
# substreams actually present via getAllSubstreamsInPart(checksums, ...).
echo "size accounting:"
${CLICKHOUSE_CLIENT} -q "SELECT secondary_indices_compressed_bytes > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_legacy_minmax' AND active"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_legacy_minmax SYNC"
