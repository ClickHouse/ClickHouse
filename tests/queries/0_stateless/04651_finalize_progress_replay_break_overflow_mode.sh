#!/usr/bin/env bash
# Tags: long, no-parallel, no-random-settings, no-random-merge-tree-settings
# no-parallel: enables a global server-side failpoint that delays the trailing
# packets of every secondary (remote) query on the server, which would slow down
# unrelated concurrently-running tests.
# long: each failpoint-enabled query deliberately sleeps for a second per remote
# connection before the trailing packets are sent.
#
# The finalize-time replay of drained remote progress in `PipelineExecutor` goes
# through `ReadProgressCallback::onProgress`, which is also the place that enforces
# read and time limits. With `overflow_mode = 'break'` the callback returns false
# instead of throwing, and the regular execution path answers that by cancelling the
# source; the replay has to honour the same contract. It must do so without losing the
# statistics it was introduced to recover: a `LIMIT` query whose remote `Progress`
# arrives only during the post-cancel drain must still complete successfully, return
# the rows the `LIMIT` asked for, and report a non-zero `rows_read` - not fail, not
# lose the statistics, and not hang.
#
# The failpoint `tcp_handler_sleep_before_secondary_query_trailing_packets` makes the
# window deterministic: the remote servers send their data blocks, then sleep before
# the trailing `Progress`, so the crossing of `max_rows_to_read` on the initiator can
# only be observed by the finalize-time replay.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_NAME="t_04651_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ${TABLE_NAME}"
# 1000 rows fit into a single granule (default index_granularity = 8192), so the whole
# table is read as one unit: the read is never interrupted in the middle, the `LIMIT`
# is always satisfied, and the drained progress always exceeds `max_rows_to_read` below.
$CLICKHOUSE_CLIENT --query="CREATE TABLE ${TABLE_NAME} (number UInt64) ENGINE = MergeTree ORDER BY number"
$CLICKHOUSE_CLIENT --query="INSERT INTO ${TABLE_NAME} SELECT * FROM system.numbers LIMIT 1000"

PR_SETTINGS="enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_for_non_replicated_merge_tree=1"
REMOTE_TABLE="remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.${TABLE_NAME})"

function check_break_mode_query()
{
    local label=$1
    local output=$2
    local rows
    local rows_read
    rows=$(echo "$output" | grep -c '"number":')
    rows_read=$(echo "$output" | grep -o '"rows_read": [0-9]*' | grep -o '[0-9]*')
    if [ "$rows" = "10" ] && [ -n "$rows_read" ] && [ "$rows_read" -gt 0 ]; then
        echo "${label} OK"
    else
        echo "${label} FAIL: rows=${rows} rows_read=${rows_read}"
    fi
}

# Make sure the global failpoint does not leak into subsequent tests even if this
# test fails in the middle.
trap '$CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT tcp_handler_sleep_before_secondary_query_trailing_packets"' EXIT

$CLICKHOUSE_CLIENT --query="SYSTEM ENABLE FAILPOINT tcp_handler_sleep_before_secondary_query_trailing_packets"

# `max_rows_to_read` is crossed on the initiator only by the trailing progress that arrives
# during the drain, so `onProgress` reports "stop" from the finalize-time replay.
BREAK_ROWS="max_rows_to_read=500, read_overflow_mode='break'"

OUTPUT=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT JSON SETTINGS ${PR_SETTINGS}, ${BREAK_ROWS}")
check_break_mode_query "parallel replicas read_overflow_mode break" "$OUTPUT"

OUTPUT=$($CLICKHOUSE_CLIENT --query="SELECT number FROM ${REMOTE_TABLE} LIMIT 10 FORMAT JSON SETTINGS ${BREAK_ROWS}")
check_break_mode_query "remote read_overflow_mode break" "$OUTPUT"

OUTPUT=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT number FROM ${TABLE_NAME} LIMIT 10 FORMAT JSON SETTINGS ${PR_SETTINGS}, ${BREAK_ROWS}")
check_break_mode_query "parallel replicas read_overflow_mode break HTTP" "$OUTPUT"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS ${TABLE_NAME}"
