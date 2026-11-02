#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# - no-parallel: the test toggles the server-global failpoint `merge_task_projection_stage_pause`.
# - no-fasttest: failpoints are not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SHOW MERGES [NOT] [I]LIKE` filters a running merge by table name; an empty pattern matches no table.

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_show_merges_like SYNC" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_show_merges_like SYNC"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_show_merges_like (k UInt64, PROJECTION p (SELECT count())) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES t_show_merges_like"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_show_merges_like SELECT number FROM numbers(10)"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_show_merges_like SELECT number FROM numbers(10)"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES t_show_merges_like"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_show_merges_like FINAL" &
optimize_pid=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT merge_task_projection_stage_pause PAUSE"

for filter in "" "LIKE 't_show_merges_like'" "ILIKE 'T_SHOW_MERGES_%'" "NOT LIKE 't_show_merges_like'" "NOT ILIKE 'T_SHOW_MERGES_%'" "LIKE ''" "NOT LIKE ''"; do
    echo "SHOW MERGES${filter:+ $filter}: $($CLICKHOUSE_CLIENT --query "SHOW MERGES $filter" | cut -f1 | grep -c -x 't_show_merges_like')"
done

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
wait $optimize_pid
