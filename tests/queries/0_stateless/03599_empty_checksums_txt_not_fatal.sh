#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-object-storage

# Regression test for a data-loss bug: a CHECK TABLE checksums.txt repair (and the
# loadChecksums backfill) rewrote checksums.txt in place without fsync, so a power loss could
# leave a zero-byte checksums.txt. An empty checksums.txt used to throw on load and detach the
# whole part as broken, losing every row of an otherwise-intact part. It must instead be treated
# like an absent checksums.txt and recalculated from the data files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_checksums"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_checksums (a UInt64, s String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_empty_checksums SELECT number, toString(number) FROM numbers(1000)"

echo "rows before:"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_empty_checksums"

DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_empty_checksums' AND active")
# ensure the path is absolute before touching it
${CLICKHOUSE_CLIENT} --query "SELECT throwIf(substring('${DATA_PATH}', 1, 1) != '/', 'Path is relative: ${DATA_PATH}')" > /dev/null || exit 1

# ---- Case 1: empty (zero-byte) checksums.txt must NOT brick the part (the bug) ----
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_checksums"
: > "${DATA_PATH}checksums.txt"
echo "checksums.txt size after truncation: $(stat -c%s "${DATA_PATH}checksums.txt")"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_checksums" 2>/dev/null
echo "rows after empty checksums.txt reload:"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_empty_checksums"
echo "checksums.txt recalculated (size > 0): $([ "$(stat -c%s "${DATA_PATH}checksums.txt")" -gt 0 ] && echo 1 || echo 0)"

# ---- Case 2: absent checksums.txt still self-heals (regression guard) ----
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_checksums"
rm -f "${DATA_PATH}checksums.txt"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_checksums" 2>/dev/null
echo "rows after absent checksums.txt reload:"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_empty_checksums"

# ---- Case 3: CHECK TABLE repairs a missing checksums.txt (recount-and-write path), fsyncs the
#      write, and the repaired file loads cleanly afterwards. Remove the file WHILE ATTACHED so
#      CHECK TABLE (not a reload) takes the recount-and-write branch. ----
rm -f "${DATA_PATH}checksums.txt"
check_query_id="check-repair-${CLICKHOUSE_DATABASE}"
echo "check table result:"
${CLICKHOUSE_CLIENT} --query_id "${check_query_id}" --query "CHECK TABLE t_empty_checksums SETTINGS check_query_single_value_result = 1"
echo "checksums.txt non-empty after CHECK TABLE: $([ "$(stat -c%s "${DATA_PATH}checksums.txt")" -gt 0 ] && echo 1 || echo 0)"
# The repair write must be fsynced (this is the durability the fix adds): FileSync > 0 for the query.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
echo "check table repair fsynced (FileSync > 0):"
${CLICKHOUSE_CLIENT} --param_check_query_id "${check_query_id}" --query "
    SELECT ProfileEvents['FileSync'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = {check_query_id:String}
      AND type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
    ORDER BY event_time DESC LIMIT 1"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_checksums"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_checksums" 2>/dev/null
echo "rows after CHECK TABLE repair and reload:"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_empty_checksums"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_empty_checksums"
