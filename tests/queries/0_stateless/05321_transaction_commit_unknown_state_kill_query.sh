#!/usr/bin/env bash
# Tags: no-ordinary-database, no-encrypted-storage, no-parallel, no-fasttest
# KILL QUERY ends a COMMIT's wait for a lost Keeper reply; the transaction is finalized later.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_commit_kill;
    CREATE TABLE t_commit_kill (n Int64) ENGINE = MergeTree ORDER BY n;
"

trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_hold_unknown_state" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_force_unknown_state_after_commit" 2>/dev/null || true
' EXIT

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_force_unknown_state_after_commit"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_hold_unknown_state"

tx 1 "BEGIN TRANSACTION"
tx 1 "INSERT INTO t_commit_kill VALUES (1)"

commit_output="${CLICKHOUSE_TMP}/commit_${CLICKHOUSE_TEST_UNIQUE_NAME}.out"
tx_async 1 "COMMIT" > "$commit_output" 2>&1

commit_query_ids="query_id LIKE '${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}_tx1_%'"
for _ in {1..600}; do
    [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE $commit_query_ids") -gt 0 ]] && break
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "SELECT 'commit_waiting', count() FROM system.processes WHERE $commit_query_ids"

timeout 30 $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE $commit_query_ids SYNC FORMAT Null" \
    && echo "kill_returned"
tx_wait 1
grep -c "Stopped waiting for the status of transaction .* because the query was cancelled.*UNKNOWN_STATUS_OF_TRANSACTION" "$commit_output"

tx 1 "BEGIN TRANSACTION"
tx 1 "ROLLBACK"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_hold_unknown_state"
for _ in {1..600}; do
    [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM t_commit_kill") -gt 0 ]] && break
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "SELECT 'finalized_later', count() FROM t_commit_kill"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_commit_kill"
rm -f "$commit_output"
