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
# rollback cleanup queued, and keeps going when one record of the batch already exists.
# Expected behaviour source: the comments of invalidateTID and storePendingInvalidTids in
# src/Interpreters/TransactionSession.cpp ("Keep the entries queued until Keeper confirms the
# write", "The Multi is all-or-nothing, so one already-existing record fails the whole batch and
# nothing is created. Retry each on its own so it doesn't block the rest.").
#
# TransactionManager::rollbackTransaction offers the failpoint smt_force_txn_rollback_invalidation
# "so tests can drive the TID-invalidation path deterministically without a real Keeper fault". For
# a whole-transaction record the znode name is exactly the tid_hash, so it is addressable from SQL.
# The records are written by the background updating thread, so the checks below are bounded polls;
# a committed transaction appends a log entry that wakes that thread right away.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

INVALID_TIDS="/test/clickhouse/txn/invalid_tids"
POLL_INTERVAL=0.1
POLL_TRIES=100

# The failpoint is enabled only around the rollbacks: it turns the rollback of every background
# transaction into a record too, so a wider window would leave unrelated znodes behind.
ON="SYSTEM ENABLE FAILPOINT smt_force_txn_rollback_invalidation;"
OFF="SYSTEM DISABLE FAILPOINT smt_force_txn_rollback_invalidation;"
POKE="BEGIN TRANSACTION; INSERT INTO txn_invalid_tid SELECT -1; COMMIT;"
# system.transactions is not transactional, and reading our own row is the only way to learn the
# record name before the transaction ends.
NO_THROW="SET throw_on_unsupported_query_inside_transaction = 0;"
# A fail point lives in the server process, so clear it on every path out of the test, not only
# on the one that reaches the last line.
trap '$CLICKHOUSE_CLIENT -q "$OFF" > /dev/null 2>&1' EXIT

function wait_for_record()
{
    local name=$1
    local i
    for ((i = 0; i < POLL_TRIES; i++)); do
        if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$INVALID_TIDS' AND name = '$name'")" = "1" ]; then
            return
        fi
        sleep "$POLL_INTERVAL"
    done
}

# Case 1: no record of the batch exists yet, so the batched Multi creates it.
read -r tid_a csn_a local_a < <($CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS txn_invalid_tid;
CREATE TABLE txn_invalid_tid (n Int64) ENGINE = MergeTree ORDER BY n;
$NO_THROW
$ON
BEGIN TRANSACTION;
INSERT INTO txn_invalid_tid SELECT 1;
SELECT tid_hash, tid.1, tid.2 FROM system.transactions WHERE tid = transactionID();
ROLLBACK;
$OFF
$POKE")
wait_for_record "$tid_a"

# Case 2: two rollbacks in one batch, and the record of the first one is already in Keeper. The
# Multi is all-or-nothing, so it fails with ZNODEEXISTS and creates nothing; the per-record retry
# must still store the second one. A copy of the record of case 1 keeps the pre-created znode
# parsable, so the normal eviction path can still reclaim it.
{ read -r tid_b1; read -r tid_b2; } < <($CLICKHOUSE_CLIENT -q "
$NO_THROW
$ON
BEGIN TRANSACTION;
INSERT INTO txn_invalid_tid SELECT 2;
INSERT INTO system.zookeeper (path, name, value) SELECT '$INVALID_TIDS', toString(tid_hash),
    (SELECT value FROM system.zookeeper WHERE path = '$INVALID_TIDS' AND name = '$tid_a')
FROM system.transactions WHERE tid = transactionID();
SELECT tid_hash FROM system.transactions WHERE tid = transactionID();
ROLLBACK;
BEGIN TRANSACTION;
INSERT INTO txn_invalid_tid SELECT 3;
SELECT tid_hash FROM system.transactions WHERE tid = transactionID();
ROLLBACK;
$OFF
$POKE")
# The second record can only appear once the batch that also holds the first one was processed.
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
