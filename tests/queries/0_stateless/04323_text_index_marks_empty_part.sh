#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree, no-object-storage
#
# Regression test for the bug fixed by adding the `marks_count == 0`
# short-circuit in `MergeTreeMarksLoader::loadMarksImpl`: a part with zero
# granules whose marks file on disk has any non-zero size made
# `loadMarksImpl` open the file and either fail size validation
# (uncompressed marks: `Bad size of marks file ...`) or fail the post-loop
# `eof()` check inside `CompressedReadBufferFromFile` (compressed marks:
# `Too many marks in file ...`).
#
# CI hits the compressed-marks variant when stress kills the server mid-merge
# and a partial `skp_idx_*.cmrk4` is left on disk while the part metadata
# records zero granules. That reproduction is non-deterministic without
# stress; instead we drive the same code path deterministically by:
#
#  1. Creating a `MergeTree` with `compress_marks = 0` so the skip-index
#     mark file is uncompressed (`skp_idx_idx.mrk4`). With uncompressed
#     marks the size check at the top of `loadMarksImpl` is the trigger,
#     and a single stale byte is enough to fire it. The compressed path
#     reaches the same fix via the same early-return.
#  2. Producing a part with zero rows / zero granules (`OPTIMIZE FINAL` then
#     `ALTER ... DELETE WHERE 1`) - the active part has `marks_count == 0`
#     and a zero-byte mark file.
#  3. Detaching the table, appending one byte to the active part's
#     `skp_idx_idx.mrk4`, removing `checksums.txt` so the part recomputes
#     them on next load (skipping the checksums-mismatch path), then
#     re-attaching.
#
# Without the fix, the `ATTACH TABLE` triggers
#   `Bad size of marks file '...skp_idx_idx.mrk4': 1, must be: 0`
# (CORRUPTED_DATA, code 246) when the secondary-index mark loader runs.
# With the fix `loadMarksImpl` returns immediately on `marks_count == 0`,
# the file is never opened, and `ATTACH TABLE` succeeds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_text_idx_empty SYNC"

# `compress_marks = 0` produces uncompressed `*.mrk4` mark files.  The
# `text` skip index over a `FixedString` column with a huge `GRANULARITY`
# is the same shape that triggered the original CIDB regression - what
# matters is that the resulting active part records `marks_count == 0`.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_text_idx_empty
(
    s FixedString(37),
    INDEX idx s TYPE text(tokenizer = array()) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS prewarm_mark_cache = true, compress_marks = 0"

# A few small inserts followed by `OPTIMIZE FINAL` and `ALTER ... DELETE WHERE 1`
# leaves a single active part with zero rows and zero granules.
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_text_idx_empty SELECT toFixedString(toString(number), 37) FROM numbers(5)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_text_idx_empty SELECT toFixedString(toString(number + 5), 37) FROM numbers(5)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_text_idx_empty FINAL"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_text_idx_empty DELETE WHERE 1 SETTINGS mutations_sync = 2"

DATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 't_text_idx_empty'")
PART_NAME=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_text_idx_empty' AND active")
PART_DIR="${DATA_PATH}${PART_NAME}"

# Sanity: the active part has zero rows / zero granules and an empty
# secondary-index mark file.  This is the exact "marks_count == 0" shape
# the fix protects.
${CLICKHOUSE_CLIENT} -q "SELECT rows, marks FROM system.parts WHERE database = currentDatabase() AND table = 't_text_idx_empty' AND active"

${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_text_idx_empty"

# Append one byte to the secondary-index mark file so the on-disk size no
# longer matches the part's recorded `marks_count = 0`.  Drop
# `checksums.txt` so loading the part on next attach recomputes checksums
# from disk instead of failing the prior-checksum verification.
printf '\x00' >> "${PART_DIR}/skp_idx_idx.mrk4"
rm "${PART_DIR}/checksums.txt"

# Without the fix: ATTACH throws `Bad size of marks file ...mrk4: 1, must be: 0`
# (CORRUPTED_DATA, code 246) and the table fails to come back online.
# With the fix: ATTACH returns cleanly and the (empty) data is still readable.
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_text_idx_empty"

${CLICKHOUSE_CLIENT} -q "SYSTEM PREWARM MARK CACHE t_text_idx_empty"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_text_idx_empty"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_text_idx_empty WHERE has(['anything'], s)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_text_idx_empty SYNC"
