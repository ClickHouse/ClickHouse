#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-parallel, no-fasttest
# no-parallel: enables a global pauseable failpoint that would pause every transaction commit.

# `KILL MUTATION` racing the commit of a transactional mutation: the kill erases the entry from
# `current_mutations_by_version` after the transaction log has committed, so `afterCommit` finds
# no entry to store the CSN into. `StorageMergeTree::setMutationCSN` must log a warning and
# return, not throw `LOGICAL_ERROR` out of the `noexcept` callback and terminate the server.
# `transaction_pause_before_mutation_csn_store` holds `afterCommit` open for the kill.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_pause_before_mutation_csn_store" ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mutation_killed_during_commit SYNC" ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mutation_killed_during_commit SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mutation_killed_during_commit (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mutation_killed_during_commit SELECT number, 0 FROM numbers(100)"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_pause_before_mutation_csn_store"

tx 1 "begin transaction" > /dev/null
# Inside a transaction the ALTER waits for its mutation, so the mutated parts exist before COMMIT.
tx 1 "alter table t_mutation_killed_during_commit update value = value + 1 where 1" > /dev/null
tx_async 1 "commit" > "$CLICKHOUSE_TMP"/05338_commit_out.txt 2>&1

# The transaction log has committed; `afterCommit` is paused before the CSN store.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT transaction_pause_before_mutation_csn_store PAUSE"

$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_mutation_killed_during_commit' SYNC FORMAT Null"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_pause_before_mutation_csn_store"
tx_wait 1

# COMMIT must succeed: the parts were mutated and committed, only the CSN of the mutation record is lost with the record.
echo "commit_errors $(grep -c 'Exception' "$CLICKHOUSE_TMP"/05338_commit_out.txt)"
echo "sum_after_commit $($CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM t_mutation_killed_during_commit")"
echo "mutations_left $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mutation_killed_during_commit'")"

# The race was taken: the CSN store found the entry gone.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
echo "warning_logged $($CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.text_log WHERE logger_name LIKE '%t_mutation_killed_during_commit%' AND message LIKE 'Mutation % was killed before its CSN % could be stored'")"

# The table keeps working: a later mutation applies on top of the committed one.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_mutation_killed_during_commit UPDATE value = value + 7 WHERE 1 SETTINGS mutations_sync = 1"
echo "sum_after_mutation $($CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM t_mutation_killed_during_commit")"
