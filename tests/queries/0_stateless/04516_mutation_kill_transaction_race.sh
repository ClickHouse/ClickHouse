#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-parallel, zookeeper
# no-parallel: enables a global pauseable failpoint that would pause mutation
# registration in concurrently running tests.

# Regression test for the race between mutation registration and transaction rollback.
# `prepareMutationEntry` adds the mutation to the transaction before
# `startMutation` registers it in `current_mutations_by_version`. A rollback in
# that window (e.g. KILL TRANSACTION) called `killMutation` before the entry existed,
# found nothing to remove, and the entry registered afterwards was orphaned: its
# transaction was gone and its CSN could never be assigned. Background jobs then raised
# an exception ("Cannot find transaction ... that has started mutation ...") and all
# subsequent mutations of the affected parts were blocked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_register_mutation" ||:
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mutation_kill_txn_race SYNC" ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mutation_kill_txn_race SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mutation_kill_txn_race (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mutation_kill_txn_race SELECT number, 0 FROM numbers(100)"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_pause_before_register_mutation"

tx 1 "begin transaction" > /dev/null
tid=$(tx 1 "select transactionID()" | cut -f2)
if [ -z "$tid" ]; then echo "FAIL: could not get transaction id"; exit 1; fi

# The ALTER pauses at the failpoint: the mutation is already added to the transaction,
# but not yet registered in the table's mutation map.
tx_async 1 "alter table t_mutation_kill_txn_race update value = value + 1 where 1" > "$CLICKHOUSE_TMP"/04516_alter_out.txt 2>&1

# Block until the ALTER is actually paused at the failpoint, then kill the transaction.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_pause_before_register_mutation PAUSE"

$CLICKHOUSE_CLIENT -q "KILL TRANSACTION WHERE tid = $tid" > /dev/null

# Make sure the rollback fully finished (its killMutation ran and found nothing)
# before letting the ALTER resume and register the entry.
rolled_back=0
for _ in {1..600}
do
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.transactions WHERE tid = $tid")" = "0" ]
    then
        rolled_back=1
        break
    fi
    sleep 0.1
done
if [ "$rolled_back" != "1" ]; then echo "FAIL: transaction did not roll back"; exit 1; fi

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_register_mutation"
tx_wait 1

# The ALTER must fail with INVALID_TRANSACTION instead of leaving an orphaned mutation.
echo "invalid_transaction_errors $(grep -c INVALID_TRANSACTION "$CLICKHOUSE_TMP"/04516_alter_out.txt)"

# No mutation may be left behind, and the rolled-back mutation must not have been applied.
echo "mutations_left $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mutation_kill_txn_race'")"
echo "sum_after_kill $($CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM t_mutation_kill_txn_race")"

# Subsequent mutations must not be blocked by the removed one.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_mutation_kill_txn_race UPDATE value = value + 7 WHERE 1 SETTINGS mutations_sync = 1"
echo "sum_after_mutation $($CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM t_mutation_kill_txn_race")"
