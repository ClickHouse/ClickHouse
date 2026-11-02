#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY: a merge that takes a part whose INSERT has not reached its commit point waits for that
# commit, then merges.
# Red if OPTIMIZE fails with SERIALIZATION_ERROR (`optimize_committed` 0) instead of waiting.
# no-parallel: `unique_key_insert_pause_before_commit` is server-wide.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_unique_key 1"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT unique_key_insert_pause_before_commit" 2>/dev/null || true
}
trap cleanup EXIT

# Returns once query `$1` has logged a message starting with `$2`. From the server's log: OPTIMIZE
# sends its logs to the client only when it finishes.
wait_for_log() {
    for _ in {1..240}; do
        logged=$($CLICKHOUSE_CLIENT --query "
            SYSTEM FLUSH LOGS text_log;
            SELECT count() FROM system.text_log WHERE query_id = '$1' AND startsWith(message, '$2')")
        [[ "$logged" -gt 0 ]] && return
        sleep 0.5
    done
    echo "query $1 never logged '$2'"
    exit 1
}

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_merge_sources"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_merge_sources (id UInt64, v String)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id) SETTINGS merge_selector_algorithm = 'Manual'"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_merge_sources SELECT number, 'a' FROM numbers(3)"

# The INSERT's part is active, and the INSERT holds its partition, until the pause is released.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT unique_key_insert_pause_before_commit"
$CLICKHOUSE_CLIENT --async_insert 0 --query "INSERT INTO uk_merge_sources SELECT 1, 'b'" &
insert_pid=$!
$CLICKHOUSE_CLIENT --max_execution_time 120 --query "
    SYSTEM WAIT FAILPOINT unique_key_insert_pause_before_commit PAUSE"

optimize_id="${CLICKHOUSE_DATABASE}_optimize"
$CLICKHOUSE_CLIENT --query_id "$optimize_id" --query "OPTIMIZE TABLE uk_merge_sources FINAL" 2>/dev/null &
optimize_pid=$!
wait_for_log "$optimize_id" "Selected 2 parts"
$CLICKHOUSE_CLIENT --query "
    SELECT 'optimize_waiting', count() FROM system.processes WHERE query_id = '$optimize_id'"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT unique_key_insert_pause_before_commit"
wait "$insert_pid" && echo "insert_committed 1" || echo "insert_committed 0"
wait "$optimize_pid" && echo "optimize_committed 1" || echo "optimize_committed 0"

$CLICKHOUSE_CLIENT --query "
    SELECT 'parts', count(), sum(rows) FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_merge_sources' AND active"
$CLICKHOUSE_CLIENT --query "SELECT id, v FROM uk_merge_sources ORDER BY id"
$CLICKHOUSE_CLIENT --query "DROP TABLE uk_merge_sources"
