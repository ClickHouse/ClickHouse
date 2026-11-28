#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY: a read sees an INSERT whose commit reply is lost with the rows it overwrote gone.
# no-parallel: `transaction_force_unknown_state_after_commit` and `transaction_hold_unknown_state`
# are server-wide.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_unique_key 1"

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT transaction_force_unknown_state_after_commit" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT transaction_hold_unknown_state" 2>/dev/null || true
}
trap cleanup EXIT

# Runs INSERT `$2` in the background as query `$1`. Synchronous and INSERT SELECT, so its server logs
# come back here while it runs.
start_insert() {
    $CLICKHOUSE_CLIENT --async_insert 0 --send_logs_level trace --query_id "$1" --query "$2" \
        >/dev/null 2>"${CLICKHOUSE_TMP}/$1.log" &
}

# Returns once query `$1` has logged `$2`.
wait_for_log() {
    for _ in {1..240}; do
        grep -q "$2" "${CLICKHOUSE_TMP}/$1.log" && return
        sleep 0.5
    done
    echo "query $1 never logged '$2'"
    exit 1
}

# Leaves INSERT `$2` (query `$1`) undetermined: its commit lands, the reply is dropped, and
# `transaction_hold_unknown_state` keeps the transaction log from resolving it.
strand_insert() {
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT transaction_hold_unknown_state"
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT transaction_force_unknown_state_after_commit"
    start_insert "$1" "$2"
    STRANDED_PID=$!
    wait_for_log "$1" "Connection lost on attempt to commit transaction"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT transaction_force_unknown_state_after_commit"
}

# Red if a read while the INSERT is undetermined returns its new row and the row it overwrote
# (`held` shows both `1 a` and `1 b`, count 4).
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_read_lost_reply"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_read_lost_reply (id UInt64, v String)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_read_lost_reply SELECT number, 'a' FROM numbers(3)"

strand_insert "${CLICKHOUSE_DATABASE}_lost" "INSERT INTO uk_read_lost_reply SELECT 1, 'b'"
$CLICKHOUSE_CLIENT --query "SELECT 'held', id, v FROM uk_read_lost_reply ORDER BY id, v"
$CLICKHOUSE_CLIENT --query "SELECT 'held count', count() FROM uk_read_lost_reply"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT transaction_hold_unknown_state"
wait "$STRANDED_PID" && echo "committed 1" || echo "committed 0"
$CLICKHOUSE_CLIENT --query "SELECT 'released', id, v FROM uk_read_lost_reply ORDER BY id, v"
$CLICKHOUSE_CLIENT --query "SELECT 'released count', count() FROM uk_read_lost_reply"
$CLICKHOUSE_CLIENT --query "DROP TABLE uk_read_lost_reply"
