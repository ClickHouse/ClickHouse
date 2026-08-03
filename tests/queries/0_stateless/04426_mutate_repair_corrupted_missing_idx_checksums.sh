#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
#
# Regression for the migration case flagged on PR #109616 (issue #109595).
# The pre-#109595 releases (26.3+) rewrote a mutated `Wide` part's `checksums.txt`
# WITHOUT the hardlinked skip-index files, producing a corrupted part shape:
# skp_idx_<name>.* files present on disk, but no per-file entries in
# `checksums.txt`. Such an index is already dead (reads probe checksums and see
# nothing, so it never prunes; `CHECK TABLE` fails with
# `UNEXPECTED_FILE_IN_DATA_PART`). A later full-part-rewrite mutation used to take
# the preserve path (`getAllSubstreamsInPart` over checksums returns no substreams)
# and drop the orphan files without repairing the part, losing the index
# forever. The fix detects the shape (index present on disk but unresolvable
# from checksums) and forces a recalculate so the writer rebuilds the index.
#
# no-fasttest: local-disk part-file surgery (see 04402/04404).
# no-object-storage/-shared/-replicated: relies on local on-disk file layout.
# no-random-merge-tree-settings: depends on a fixed granule count and the
# standalone (non-packed) index file that the surgery injects; the CREATE below
# pins `packed_skip_index_max_bytes` = 0 because the tag does not cover a
# non-zero server default.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_corrupt_minmax SYNC"

# v = k is monotone, so a minmax index over v prunes a point query to a single
# granule. `index_granularity` = 100 over 2000 rows gives 20 granules. m is a
# MATERIALIZED column so that `DROP COLUMN` m forces a full-part rewrite
# (`MutateAllPartColumnsTask`).
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_corrupt_minmax
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
         columns_and_secondary_indices_sizes_lazy_calculation = 0"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_corrupt_minmax (k, v) SELECT number, number FROM numbers(2000)"

DATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 't_corrupt_minmax'")
ACTIVE_PART=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_corrupt_minmax' AND active LIMIT 1")

# Save the freshly written index files, then drop and re-declare the index so
# the active part has NO skp_idx entries in `checksums.txt`, and re-inject the
# saved files. This reproduces the released-bug shape without depending on an
# old binary: index files on disk, missing from `checksums.txt`.
cp "${ACTIVE_PART}skp_idx_mm_v.idx2" "${DATA_PATH}/saved_mm_v.idx2"
cp "${ACTIVE_PART}skp_idx_mm_v.cmrk2" "${DATA_PATH}/saved_mm_v.cmrk2"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_corrupt_minmax DROP INDEX mm_v SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_corrupt_minmax ADD INDEX mm_v v TYPE minmax GRANULARITY 1"

CORRUPT_PART=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_corrupt_minmax' AND active LIMIT 1")
cp "${DATA_PATH}/saved_mm_v.idx2" "${CORRUPT_PART}skp_idx_mm_v.idx2"
cp "${DATA_PATH}/saved_mm_v.cmrk2" "${CORRUPT_PART}skp_idx_mm_v.cmrk2"

# The corrupted index is already dead: files are on disk but not in checksums,
# so it does not prune and `CHECK TABLE` fails.
echo "corrupted_prunes:"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_corrupt_minmax WHERE v = 1042) WHERE explain ILIKE '%Granules: 1/20%'"

# Full-part rewrite (`MutateAllPartColumnsTask` via `DROP COLUMN` of a MATERIALIZED
# column). Before the fix the preserve path found no substreams in checksums and
# dropped the orphan files, permanently losing the index. The fix forces a
# recalculate so the writer rebuilds the index from column data.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_corrupt_minmax DROP COLUMN m SETTINGS mutations_sync = 2"

NEW_PART=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_corrupt_minmax' AND active LIMIT 1")
echo "current_idx2_present:"
if ls "${NEW_PART}skp_idx_mm_v.idx2" >/dev/null 2>&1; then echo 1; else echo 0; fi

# The repaired index must prune to one granule, `CHECK TABLE` must pass, and the
# on-disk size accounting must include the rebuilt index.
echo "repaired_prunes:"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_corrupt_minmax WHERE v = 1042) WHERE explain ILIKE '%Granules: 1/20%'"
echo "check_table:"
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_corrupt_minmax SETTINGS check_query_single_value_result = 1"
echo "index_size_nonzero:"
${CLICKHOUSE_CLIENT} -q "SELECT secondary_indices_compressed_bytes > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_corrupt_minmax' AND active LIMIT 1"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_corrupt_minmax WHERE v = 1042"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_corrupt_minmax SYNC"
