#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
#
# `no-fasttest`: local-disk part-file surgery is not reliably available on the Fast test macOS runner.
# `no-object-storage` / `no-shared-merge-tree` / `no-replicated-database`: the fixture edits real
# local part files and relies on ATTACH recomputing `checksums.txt` from them.
# `no-random-merge-tree-settings`: the fixture targets a standalone index file at a fixed granule
# count; the settings it needs are pinned in the CREATE below.
#
# Mixed-format part: one index carrying BOTH a legacy `.idx` and a fresh `.idx2`, as an
# intermediate buggy build could leave it. Cleanup keyed on the preferred read layout never saw
# the stale `.idx`, so it was hardlinked forward with a stale checksum and the part stayed mixed
# forever. Issue #109595.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_mixed_minmax SYNC"

# v = k is monotone, so the minmax index over v prunes a point query to a single
# granule. `index_granularity` = 100 over 2000 rows gives 20 granules.
${CLICKHOUSE_CLIENT} -q "
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
         columns_and_secondary_indices_sizes_lazy_calculation = 0"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_mixed_minmax (k, v) SELECT number, number FROM numbers(2000)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_mixed_minmax FINAL"

# Detach so we can rewrite the part files, then fabricate the MIXED layout: keep
# the modern ".idx2" AND add a stale legacy ".idx" copy (for a non-nullable
# column the v1 and v2 minmax payloads are byte-identical), then drop checksums
# so ATTACH recomputes them to include both files.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_mixed_minmax DETACH PARTITION tuple() SETTINGS mutations_sync = 2"

DATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 't_mixed_minmax'")
PART_DIR=$(find "${DATA_PATH}detached" -maxdepth 1 -type d -name 'all_*' | head -1)

cp "${PART_DIR}/skp_idx_mm_v.idx2" "${PART_DIR}/skp_idx_mm_v.idx"
rm -f "${PART_DIR}/checksums.txt"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_mixed_minmax ATTACH PARTITION tuple()"

# Sanity: the part carries BOTH files, and the index still prunes to one granule
# (the reader prefers ".idx2").
echo "before_mixed:"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_mixed_minmax WHERE v = 42) WHERE explain ILIKE '%Granules: 1/20%'"

# Pre-cleanup size accounting: the mixed part carries BOTH data files, so the
# reported index size must sum both (union walk), not only the preferred ".idx2".
# The shared ".cmrk2" mark file is counted exactly once.
#
# Read this from `system.data_skipping_indices`, not from `system.parts`: the
# `system.parts` secondary-index columns are served by the part-lifetime
# `total_secondary_indices_size` accumulator, which is never reset, so an ATTACHed
# part reports every size doubled. That is independent of this test (it reproduces
# on unmodified master for a plain single-".idx2" part) and is tracked separately.
MIXED_PART=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_mixed_minmax' AND active ORDER BY name LIMIT 1")
DISK_DATA=$(( $(stat -c%s "${MIXED_PART}/skp_idx_mm_v.idx") + $(stat -c%s "${MIXED_PART}/skp_idx_mm_v.idx2") ))
MRK_FILE=$(find "${MIXED_PART}" -maxdepth 1 -name 'skp_idx_mm_v.*mrk*' | head -1)
DISK_MARKS=$(stat -c%s "${MRK_FILE}")
echo "size_counts_both_idx_payloads:"
${CLICKHOUSE_CLIENT} -q "SELECT data_compressed_bytes = ${DISK_DATA} FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_mixed_minmax' AND name = 'mm_v'"
echo "size_counts_marks_once:"
${CLICKHOUSE_CLIENT} -q "SELECT marks_bytes = ${DISK_MARKS} FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_mixed_minmax' AND name = 'mm_v'"

# --- Case 1: rebuild mutation (`ALTER UPDATE` of the indexed column) ---
# The writer produces a fresh ".idx2". The stale ".idx" must be stripped, not
# hardlinked forward.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_mixed_minmax UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2"

NEW_PART_DIR=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_mixed_minmax' AND active ORDER BY name LIMIT 1")
echo "rebuild_stale_idx_leaked:"
if ls "${NEW_PART_DIR}/skp_idx_mm_v.idx" >/dev/null 2>&1; then echo 1; else echo 0; fi
echo "rebuild_idx2_present:"
if ls "${NEW_PART_DIR}/skp_idx_mm_v.idx2" >/dev/null 2>&1; then echo 1; else echo 0; fi
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_mixed_minmax SETTINGS check_query_single_value_result = 1"
echo "rebuild_prunes:"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_mixed_minmax WHERE v = 42) WHERE explain ILIKE '%Granules: 1/20%'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_mixed_minmax SYNC"

# --- Case 2: `DROP INDEX` on a mixed part (fresh table) ---
# `DROP INDEX` has no mutation pipeline; the stale ".idx" must be removed, not
# hardlinked into the new part. Use a fresh table so the fabrication starts
# from a clean (writable) source part, not a mutated one.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_mixed_minmax_drop SYNC"
${CLICKHOUSE_CLIENT} -q "
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
         columns_and_secondary_indices_sizes_lazy_calculation = 0"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_mixed_minmax_drop (k, v) SELECT number, number FROM numbers(2000)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_mixed_minmax_drop FINAL"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_mixed_minmax_drop DETACH PARTITION tuple() SETTINGS mutations_sync = 2"

DATA_PATH2=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 't_mixed_minmax_drop'")
PART_DIR2=$(find "${DATA_PATH2}detached" -maxdepth 1 -type d -name 'all_*' | head -1)
chmod u+w "${PART_DIR2}"
cp "${PART_DIR2}/skp_idx_mm_v.idx2" "${PART_DIR2}/skp_idx_mm_v.idx"
rm -f "${PART_DIR2}/checksums.txt"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_mixed_minmax_drop ATTACH PARTITION tuple()"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_mixed_minmax_drop DROP INDEX mm_v SETTINGS mutations_sync = 2, alter_sync = 2"
DROP_PART_DIR=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_mixed_minmax_drop' AND active ORDER BY name LIMIT 1")
echo "drop_stale_idx_leaked:"
if ls "${DROP_PART_DIR}/skp_idx_mm_v.idx" >/dev/null 2>&1; then echo 1; else echo 0; fi
echo "drop_idx2_leaked:"
if ls "${DROP_PART_DIR}/skp_idx_mm_v.idx2" >/dev/null 2>&1; then echo 1; else echo 0; fi
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_mixed_minmax_drop SETTINGS check_query_single_value_result = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_mixed_minmax_drop SYNC"
