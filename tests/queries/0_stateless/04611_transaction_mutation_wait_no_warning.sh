#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Tag rationale: uses explicit transactions, which require an Atomic,
# non-replicated database.

# Regression test for the read-only mutation status path streaming a spurious
# warning to the client.
#
# `waitForMutation` (used by a barrier `ALTER`, e.g. `RENAME COLUMN`, and by
# synchronous mutations) polls `getIncompleteMutationsStatus` from the client
# thread. After a transactional mutation is committed, its transaction leaves the
# running list while the finished mutation entry still lingers in
# `current_mutations_by_version`. In that state the status path used to call
# `tryGetTransactionForMutation(..., log)`, which logged
# "Cannot find transaction ... probably it finished" and streamed it to the
# client (`send_logs_level=warning`), making a barrier `ALTER` produce spurious
# `<Warning>` output. The status path must resolve this now-normal state
# silently; only the scheduling and `killMutation` paths keep the logger.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_txn_mut_wait;
    CREATE TABLE t_txn_mut_wait (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key
        SETTINGS finished_mutations_to_keep = 100, old_parts_lifetime = 3600;
"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_txn_mut_wait SELECT number, 1 FROM numbers(100)"

# Run a mutation inside an explicit transaction and commit it. The mutation
# finishes inside the transaction but its entry lingers afterwards. Capture the
# transaction id so we can wait for it to leave the running list.
tid=$($CLICKHOUSE_CLIENT --multiquery -q "
    BEGIN TRANSACTION;
    SELECT transactionID();
    ALTER TABLE t_txn_mut_wait UPDATE value = value + 1 WHERE 1;
    COMMIT;
" 2>/dev/null | head -1)

# Wait until the committed transaction has left the running list (so the status
# path below hits the "transaction already finished" case), while its finished
# mutation entry still lingers in `current_mutations_by_version`.
running=1
for _ in {1..120}; do
    running=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.transactions WHERE tid = $tid")
    [ "$running" = "0" ] && break
    sleep 0.5
done

# Fail explicitly if the transaction never left the running list: otherwise the
# barrier `ALTER` below would take the old "live transaction" path in
# `getIncompleteMutationsStatusUnlocked` and the test could print OK without ever
# exercising the committed-without-live-`txn` case it is meant to cover.
if [ "$running" != "0" ]; then
    echo "FAILED: transaction $tid did not leave system.transactions in time"
    $CLICKHOUSE_CLIENT -q "DROP TABLE t_txn_mut_wait"
    exit 1
fi

# A barrier `ALTER` (`RENAME COLUMN`) waits for the previous mutation via
# `waitForMutation` from the client thread. Assert the client receives no
# "Cannot find transaction ... probably it finished" warning. The mutation is
# already done, so the wait returns immediately.
stderr_file="${CLICKHOUSE_TMP}/04611_rename_stderr.txt"
timeout 60 $CLICKHOUSE_CLIENT --send_logs_level=warning --max_execution_time 60 \
    -q "ALTER TABLE t_txn_mut_wait RENAME COLUMN value TO value2" 2> "$stderr_file"

if grep -q "Cannot find transaction" "$stderr_file"; then
    echo "FAILED: client received the spurious warning:"
    grep "Cannot find transaction" "$stderr_file"
else
    echo "OK"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE t_txn_mut_wait"
rm -f "$stderr_file"
