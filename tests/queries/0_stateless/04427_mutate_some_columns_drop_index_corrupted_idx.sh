#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
#
# Regression for the migration case flagged on PR #109616 (issue #109595).
# 04426 covers the FULL-part-rewrite path (`MutateAllPartColumnsTask`), which
# repairs a part corrupted by the released #109595 bug (skp_idx_<name>.* on
# disk, no per-file entries in `checksums.txt`) by recomputing the index. This
# test covers the two remaining paths that a corrupted part can go through:
#   A) a some-columns mutation (`ALTER UPDATE` of a non-indexed column), and
#   B) `DROP INDEX`.
# Their bookkeeping resolved index files only through `checksums.txt`, so the
# orphan standalone files were hardlinked into the new part unchanged and
# `CHECK TABLE` kept failing. The fix drops the dead orphan files on both paths,
# leaving the part consistent (the index is simply absent from this part -- a
# later `MATERIALIZE INDEX` repopulates it; a full rewrite does not, because once
# the orphans are gone the index is no longer present on disk and so is not
# selected for recalculation).
#
# no-fasttest: local-disk part-file surgery (see 04402/04404/04426).
# no-object-storage/-shared/-replicated: relies on local on-disk file layout.
# no-random-merge-tree-settings: depends on a fixed granule count and the
# standalone (non-packed) index file that the surgery injects; both CREATEs below
# pin `packed_skip_index_max_bytes` = 0 because the tag does not cover a non-zero
# server default.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Fabricate a part in the released-bug shape: skp_idx_mm_v.* on disk but absent
# from `checksums.txt`. Save the freshly written index files, DROP+re-ADD the
# index so the active part has no skp_idx entries in checksums, then re-inject.
make_corrupted_part () {
    local tbl="$1"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${tbl} SYNC"
    ${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${tbl}
    (
        k UInt64,
        v UInt64,
        w UInt64,
        INDEX mm_v v TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             index_granularity = 100, replace_long_file_name_to_hash = 0,
             packed_skip_index_max_bytes = 0,
             columns_and_secondary_indices_sizes_lazy_calculation = 0"

    ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${tbl} (k, v, w) SELECT number, number, number FROM numbers(2000)"

    local data_path active
    data_path=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = '${tbl}'")
    active=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active LIMIT 1")

    cp "${active}skp_idx_mm_v.idx2" "${data_path}/saved_${tbl}.idx2"
    cp "${active}skp_idx_mm_v.cmrk2" "${data_path}/saved_${tbl}.cmrk2"

    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${tbl} DROP INDEX mm_v SETTINGS mutations_sync = 2"
    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${tbl} ADD INDEX mm_v v TYPE minmax GRANULARITY 1"

    local corrupt
    corrupt=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active LIMIT 1")
    cp "${data_path}/saved_${tbl}.idx2" "${corrupt}skp_idx_mm_v.idx2"
    cp "${data_path}/saved_${tbl}.cmrk2" "${corrupt}skp_idx_mm_v.cmrk2"
}

orphan_on_disk () {
    local tbl="$1"
    local part
    part=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active LIMIT 1")
    if ls "${part}"skp_idx_mm_v.* >/dev/null 2>&1; then echo 1; else echo 0; fi
}

# --- Path A: some-columns mutation (`ALTER UPDATE` of the non-indexed column w) ---
make_corrupted_part t_some
echo "A_corrupted_orphan_on_disk:"
orphan_on_disk t_some
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_some UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 2"
echo "A_orphan_after_update:"
orphan_on_disk t_some
echo "A_check_table:"
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_some SETTINGS check_query_single_value_result = 1"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_some WHERE v = 1042"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_some SYNC"

# --- Path B: `DROP INDEX` on a corrupted part ---
make_corrupted_part t_drop
echo "B_corrupted_orphan_on_disk:"
orphan_on_disk t_drop
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_drop DROP INDEX mm_v SETTINGS mutations_sync = 2"
echo "B_orphan_after_drop_index:"
orphan_on_disk t_drop
echo "B_check_table:"
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_drop SETTINGS check_query_single_value_result = 1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_drop SYNC"

# --- Path C (no regression): a healthy part keeps its index through a some-columns mutation ---
# `packed_skip_index_max_bytes` = 0 keeps this control on the standalone
# (non-packed) preserve path that paths A and B exercise; without it the control
# would assert over packed-archive preservation instead (covered by 04403).
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_ok SYNC"
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_ok (k UInt64, v UInt64, w UInt64, INDEX mm_v v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 100, replace_long_file_name_to_hash = 0,
         packed_skip_index_max_bytes = 0"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_ok (k, v, w) SELECT number, number, number FROM numbers(2000)"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_ok UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 2"
echo "C_healthy_prunes_after_update:"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ok WHERE v = 1042) WHERE explain ILIKE '%Granules: 1/20%'"
echo "C_check_table:"
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_ok SETTINGS check_query_single_value_result = 1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ok SYNC"
