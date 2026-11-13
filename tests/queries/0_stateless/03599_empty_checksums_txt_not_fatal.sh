#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-object-storage, no-parallel-replicas

# Regression test for a data-loss bug: a CHECK TABLE checksums.txt repair (and the
# loadChecksums backfill) rewrote checksums.txt in place without fsync, so a power loss could
# leave a zero-byte checksums.txt. An empty checksums.txt used to throw on load and detach the
# whole part as broken, losing every row of an otherwise-intact part. It must instead be treated
# like an absent checksums.txt and recalculated from the data files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_checksums"

# min_*_for_wide_part pins PartType, not PartStorageType. Pin Full part storage too: on Packed
# storage checksums.txt lives inside data.packed, so the manipulations below would touch an ignored
# side file and the test would pass without exercising the repaired path.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_checksums (a UInt64, s String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             min_level_for_full_part_storage = 0;
"

# The test manipulates one part's checksums.txt by an absolute path captured once, so the table must
# hold exactly one part directory with no covered sibling. Stop merges before inserting so no merge
# ever produces a covering part, and pin the block-size settings so the single insert yields one part
# regardless of CI randomization (min_insert_block_size_rows/max_block_size can otherwise split it, and
# the last value wins under --allow_repeated_settings). No OPTIMIZE: it would leave the original part
# behind as a covered directory and make the captured path ambiguous.
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_checksums"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_checksums SELECT number, toString(number) FROM numbers(1000)"

echo "rows before:"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_empty_checksums"

# Require exactly one active part; otherwise the single-path manipulation below is meaningless.
${CLICKHOUSE_CLIENT} --query "SELECT throwIf(count() != 1, 'Expected exactly one active part') FROM system.parts WHERE database = currentDatabase() AND table = 't_empty_checksums' AND active" > /dev/null || exit 1
# Fail loudly rather than vacuously if the pin above ever stops selecting Full storage.
${CLICKHOUSE_CLIENT} --query "SELECT throwIf(part_storage_type != 'Full', 'Expected Full part storage') FROM system.parts WHERE database = currentDatabase() AND table = 't_empty_checksums' AND active" > /dev/null || exit 1
DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_empty_checksums' AND active LIMIT 1")
# ensure the path is absolute before touching it
${CLICKHOUSE_CLIENT} --query "SELECT throwIf(substring('${DATA_PATH}', 1, 1) != '/', 'Path is relative: ${DATA_PATH}')" > /dev/null || exit 1

# Assertions below are made server-side (row counts, CHECK TABLE) rather than by stat-ing the on-disk
# checksums.txt: an external stat of a live part directory races the server's own reads/writes and
# cache prewarming under CI randomization. Reloading the part from disk (DETACH/ATTACH) forces the
# recovered checksums to be read back from disk, so a reload that returns all rows proves recovery
# persisted a valid checksums.txt on disk.

# ---- Case 1: empty (zero-byte) checksums.txt must NOT brick the part (the bug) ----
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_checksums"
: > "${DATA_PATH}checksums.txt"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_checksums" 2>/dev/null
echo "rows after empty checksums.txt reload:"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_empty_checksums"
# Recovery must have PERSISTED a valid checksums.txt, not just recovered it in memory. Reload the part
# from disk (so the checksums are read back from disk) and run a full CHECK TABLE: if the backfill was
# persisted, CHECK validates the existing file and reports an empty message; if it was not persisted the
# file is still empty/absent and CHECK would report "Checksums recounted and written to disk." instead.
# This proves persistence server-side, without stat-ing the live part file (which races the server).
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_checksums"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_checksums" 2>/dev/null
echo "empty checksums.txt recovered (rows after reload):"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_empty_checksums"
echo "empty checksums.txt recovered (check passed, no recount message):"
${CLICKHOUSE_CLIENT} --query "CHECK TABLE t_empty_checksums SETTINGS check_query_single_value_result = 0" | cut -f2,3

# ---- Case 2: absent checksums.txt still self-heals (regression guard) ----
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_checksums"
rm -f "${DATA_PATH}checksums.txt"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_checksums" 2>/dev/null
echo "rows after absent checksums.txt reload:"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_empty_checksums"

# ---- Case 3: CHECK TABLE repairs a missing checksums.txt (recount-and-write path) and the repaired
#      file loads cleanly afterwards. Remove the file WHILE ATTACHED so CHECK TABLE (not a reload)
#      takes the recount-and-write branch. The repaired file must survive a reload from disk: an
#      empty or partial write would fail to load and lose rows, so the reload returning all rows
#      proves the repair wrote a valid checksums.txt to disk. ----
rm -f "${DATA_PATH}checksums.txt"
echo "check table result:"
${CLICKHOUSE_CLIENT} --query "CHECK TABLE t_empty_checksums SETTINGS check_query_single_value_result = 1"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_checksums"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_checksums" 2>/dev/null
echo "rows after CHECK TABLE repair and reload:"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_empty_checksums"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_empty_checksums"
