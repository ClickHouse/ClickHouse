#!/usr/bin/env bash
# Tags: zookeeper, no-ordinary-database, no-replicated-database, no-parallel, no-fasttest
# Tag zookeeper: the invalidation records are znodes under <transaction_log.zookeeper_path>/invalid_tids.
# Tag no-ordinary-database: transactions need an Atomic database.
# Tag no-replicated-database: a Replicated database turns MergeTree into ReplicatedMergeTree, and
#   transactions do not support it.
# Tag no-parallel: SYSTEM ENABLE/DISABLE FAILPOINT is system-global, and the test writes a znode by
#   hand into the system-global invalid_tids path.
# Tag no-fasttest: a test that arms a fail point must be no-parallel, and the fast test keeps the
#   tests that run alone out (ci/jobs/check_style.py, check_failpoint_tests_are_isolated).

# Test: TransactionSession::storePendingInvalidTids writes the TID invalidations that a failed
# rollback cleanup queued, and keeps going when one record of the batch already exists. Source of
# the expected behaviour: the comment of storePendingInvalidTids in
# src/Interpreters/TransactionSession.cpp ("The Multi is all-or-nothing, so one already-existing
# record fails the whole batch and nothing is created. Retry each on its own so it doesn't block
# the rest."). rollbackTransaction offers smt_force_txn_rollback_invalidation "so tests can drive
# the TID-invalidation path deterministically without a real Keeper fault"; for a whole-transaction
# record the znode name is exactly the tid_hash, so it is addressable from SQL. The records are
# written by the background updating thread, so the checks below are bounded polls; a committed
# transaction appends a log entry that wakes that thread right away. Case 2 needs both
# invalidations queued before that thread snapshots the queue, so both rollbacks are held at the
# pauseable failpoint transaction_rollback_pause_after_mark, which stops a rollback after it marked
# the transaction and before it queues the invalidation; they are released together, and the queue
# is poked only once both ROLLBACK queries have returned.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

INVALID_TIDS="/test/clickhouse/txn/invalid_tids"
POLL_INTERVAL=0.1
POLL_TRIES=100
# FP_FORCE is on only around the rollbacks: it would record every unrelated background one too.
FP_FORCE=smt_force_txn_rollback_invalidation
FP_PAUSE=transaction_rollback_pause_after_mark
POKE="BEGIN TRANSACTION; INSERT INTO txn_invalid_tid SETTINGS async_insert = 0 SELECT -1; COMMIT;"
# Every INSERT pins async_insert = 0: an async insert inside a transaction is NOT_IMPLEMENTED.
# system.transactions is not transactional, and our own row names the record before the txn ends.
NO_THROW="SET throw_on_unsupported_query_inside_transaction = 0;"

# A fail point lives in the server process, so clear it on every path out of the test.
cleanup() {
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP_PAUSE; SYSTEM DISABLE FAILPOINT $FP_FORCE" > /dev/null 2>&1
}
trap cleanup EXIT

# Bounded poll: wait until the counting query $1 returns $2.
function wait_for_count()
{
    local i
    for ((i = 0; i < POLL_TRIES; i++)); do
        [ "$($CLICKHOUSE_CLIENT -q "$1")" = "$2" ] && return
        sleep "$POLL_INTERVAL"
    done
}

function wait_for_record()
{
    wait_for_count "SELECT count() FROM system.zookeeper WHERE path = '$INVALID_TIDS' AND name = '$1'" 1
}

function tid_hash_of()
{
    tx "$1" "SELECT tid_hash FROM system.transactions WHERE tid = transactionID()
        SETTINGS throw_on_unsupported_query_inside_transaction = 0" | awk '{print $2}'
}

# Case 1: no record of the batch exists yet, so the batched Multi creates it.
read -r tid_a csn_a local_a < <($CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS txn_invalid_tid;
CREATE TABLE txn_invalid_tid (n Int64) ENGINE = MergeTree ORDER BY n;
$NO_THROW
SYSTEM ENABLE FAILPOINT $FP_FORCE;
BEGIN TRANSACTION;
INSERT INTO txn_invalid_tid SETTINGS async_insert = 0 SELECT 1;
SELECT tid_hash, tid.1, tid.2 FROM system.transactions WHERE tid = transactionID();
ROLLBACK;
SYSTEM DISABLE FAILPOINT $FP_FORCE;
$POKE")
wait_for_record "$tid_a"

# Case 2: two rollbacks in one batch, and the record of the first one is already in Keeper, so the
# Multi fails with ZNODEEXISTS and creates nothing; the per-record retry must still store the
# second one. A copy of the record of case 1 keeps the pre-created znode parsable.
tx 1 "BEGIN TRANSACTION" > /dev/null
tid_b1=$(tid_hash_of 1)
tx 1 "INSERT INTO txn_invalid_tid SETTINGS async_insert = 0 SELECT 2" > /dev/null
tx 2 "BEGIN TRANSACTION" > /dev/null
tid_b2=$(tid_hash_of 2)
tx 2 "INSERT INTO txn_invalid_tid SETTINGS async_insert = 0 SELECT 3" > /dev/null
$CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) SELECT '$INVALID_TIDS',
    '$tid_b1', (SELECT value FROM system.zookeeper WHERE path = '$INVALID_TIDS' AND name = '$tid_a')"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP_FORCE; SYSTEM ENABLE FAILPOINT $FP_PAUSE;"
tx_async 1 "ROLLBACK" > /dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP_PAUSE PAUSE"
tx_async 2 "ROLLBACK" > /dev/null
# Both rollbacks now sit in the fail point, so both are still running queries.
wait_for_count "SELECT count() FROM system.processes
    WHERE query_id LIKE '${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}_tx%' AND query ILIKE '%rollback%'" 2
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP_PAUSE"
wait
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP_FORCE; $POKE"
wait_for_record "$tid_b2"

$CLICKHOUSE_CLIENT -q "
SELECT 'stored_fresh', count()
FROM system.zookeeper WHERE path = '$INVALID_TIDS' AND name = '$tid_a';
SELECT 'record_names_rolled_back_txn', startsWith(value, '1 ($csn_a, $local_a, ')
FROM system.zookeeper WHERE path = '$INVALID_TIDS' AND name = '$tid_a';
SELECT 'second_record_of_the_batch_stored', count()
FROM system.zookeeper WHERE path = '$INVALID_TIDS' AND name = '$tid_b2';
SELECT 'existing_record_kept_unchanged',
    (SELECT value FROM system.zookeeper WHERE path = '$INVALID_TIDS' AND name = '$tid_b1')
    = (SELECT value FROM system.zookeeper WHERE path = '$INVALID_TIDS' AND name = '$tid_a');
DROP TABLE txn_invalid_tid;"
