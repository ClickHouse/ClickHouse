#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
#
# `no-fasttest`: local-disk part-file surgery is not reliably available on the Fast test macOS runner.
# `no-object-storage` / `no-shared-merge-tree` / `no-replicated-database`: the fixture edits real
# local part files and relies on ATTACH recomputing `checksums.txt` from them.
# `no-random-merge-tree-settings`: the fixture targets a standalone index file at a fixed granule
# count; the settings it needs are pinned in the CREATE below.
#
# A full-part rewrite must keep a non-recalculated minmax index whose data file is still the
# legacy `.idx` (v1) rather than the current `.idx2` (v2); it used to hardlink the mark but not
# that data file, silently dropping the index. Issue #109595.
#
# The modern writer only emits `.idx2`, so the legacy shape is fabricated: DETACH, rename
# `.idx2` to `.idx` (byte-identical payloads for a non-nullable column), drop `checksums.txt`
# so ATTACH recomputes it, ATTACH, then mutate.

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
-- min_bytes_for_wide_part = 0 forces the Wide layout, and
-- packed_skip_index_max_bytes = 0 keeps that file standalone rather than a
-- member of skp_idx.packed, which the rename cannot reach.
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 100, replace_long_file_name_to_hash = 0,
         packed_skip_index_max_bytes = 0,
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

# Full part rewrite: `DROP COLUMN` of a MATERIALIZED column takes
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
# data file and reported `secondary_indices_compressed_bytes` = 0. The fix probes the
# substreams actually present via `getAllSubstreamsInPart`.
echo "size accounting:"
${CLICKHOUSE_CLIENT} -q "SELECT secondary_indices_compressed_bytes > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_legacy_minmax' AND active"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_legacy_minmax SYNC"
