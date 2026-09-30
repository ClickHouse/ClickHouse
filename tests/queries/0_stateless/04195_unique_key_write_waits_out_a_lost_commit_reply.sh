#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY: a write whose commit reply is lost waits until its transaction resolves.
#   1. lost reply: an INSERT and a DELETE succeed with their rows visible
#   2. next writer: an INSERT of the same key waits for the undetermined one, then succeeds
#   3. given up: once the undetermined INSERT is killed, the next INSERT of its key waits, then succeeds
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

# 1. lost reply: red if the INSERT or the DELETE reports UNKNOWN_STATUS_OF_TRANSACTION.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_lost_reply"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_lost_reply (id UInt64, v String)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_lost_reply SELECT number, 'a' FROM numbers(5)"

# The commit lands in Keeper and its reply is dropped, so the status is known only once the
# transaction log's updating thread finds the csn entry.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT transaction_force_unknown_state_after_commit"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_lost_reply SELECT number, 'b' FROM numbers(3, 4)"
$CLICKHOUSE_CLIENT --query "DELETE FROM uk_lost_reply WHERE id = 1"
$CLICKHOUSE_CLIENT --query "SELECT id, v FROM uk_lost_reply ORDER BY id"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT transaction_force_unknown_state_after_commit"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_lost_reply"

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

# Starts INSERT `$2` (query `$1`) of the stranded key and returns once it asked for the partition.
start_next_insert() {
    start_insert "$1" "$2"
    NEXT_PID=$!
    wait_for_log "$1" "waiting for the partition guard"
}

# 2. next writer: red if the second INSERT probes while the first is undetermined (a debug server
# aborts on the key live in two parts), or does not succeed once the first commits.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_next_writer"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_next_writer (id UInt64, v String)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_next_writer SELECT number, 'a' FROM numbers(3)"

strand_insert "${CLICKHOUSE_DATABASE}_first" "INSERT INTO uk_next_writer SELECT 1, 'b'"
start_next_insert "${CLICKHOUSE_DATABASE}_next" "INSERT INTO uk_next_writer SELECT 1, 'c'"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT transaction_hold_unknown_state"

wait "$STRANDED_PID" && echo "first_committed 1" || echo "first_committed 0"
wait "$NEXT_PID" && echo "next_committed 1" || echo "next_committed 0"
$CLICKHOUSE_CLIENT --query "SELECT 'next_writer', id, v FROM uk_next_writer ORDER BY id"
$CLICKHOUSE_CLIENT --query "DROP TABLE uk_next_writer"

# 3. given up: red if the next INSERT probes while the killed one is still undetermined (a debug
# server aborts on the key live in two parts), or does not succeed once it resolves (`next_committed` 0).
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_given_up"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE uk_given_up (id UInt64, v String)
    ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_given_up SELECT number, 'a' FROM numbers(3)"

strand_insert "${CLICKHOUSE_DATABASE}_killed" "INSERT INTO uk_given_up SELECT 1, 'b'"
start_next_insert "${CLICKHOUSE_DATABASE}_waiting" "INSERT INTO uk_given_up SELECT 1, 'c'"
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '${CLICKHOUSE_DATABASE}_killed' SYNC FORMAT Null"
wait "$STRANDED_PID" || true
wait_for_log "${CLICKHOUSE_DATABASE}_waiting" "waiting for an unresolved part"

# The killed INSERT's commit landed, so it resolves as committed once released.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT transaction_hold_unknown_state"
wait "$NEXT_PID" && echo "next_committed 1" || echo "next_committed 0"
$CLICKHOUSE_CLIENT --query "SELECT 'given_up', id, v FROM uk_given_up ORDER BY id"
$CLICKHOUSE_CLIENT --query "DROP TABLE uk_given_up"
