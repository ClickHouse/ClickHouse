#!/usr/bin/env bash
# Tags: replica, no-shared-merge-tree, no-replicated-database
# - replica: uses ReplicatedMergeTree
# - no-shared-merge-tree: relies on two explicit replicas + sequential quorum + STOP FETCHES to keep quorum unsatisfied
# - no-replicated-database: creates two explicit replicas (r1, r2) sharing one ZooKeeper path

# A quorum INSERT that gets killed must stop waiting for quorum promptly instead of
# blocking for the whole insert_quorum_timeout. See ReplicatedMergeTreeSink::waitForQuorum:
# the quorum watch is only signalled when the quorum node changes, so on a KILL QUERY
# nothing wakes the waiter up. Before the fix the cancelled INSERT lingered in the
# processlist for the entire (possibly huge) insert_quorum_timeout, tripping the hung-check.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS quorum_r1 SYNC;
DROP TABLE IF EXISTS quorum_r2 SYNC;
CREATE TABLE quorum_r1 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/quorum_cancel', 'r1') ORDER BY x;
CREATE TABLE quorum_r2 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/quorum_cancel', 'r2') ORDER BY x;
-- r2 stays active (so the quorum precondition passes) but never fetches the part,
-- so insert_quorum = 2 can never be satisfied and the INSERT blocks in waitForQuorum.
SYSTEM STOP FETCHES quorum_r2;
"

QUERY_ID="quorum_cancel_${CLICKHOUSE_DATABASE}_$$"

# Commit a part on r1 and wait for quorum (which never comes). insert_quorum_timeout is
# set very high so that, without prompt cancellation, the query would hang far longer
# than this test is willing to wait.
$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" \
    --insert_quorum=2 --insert_quorum_parallel=0 --insert_quorum_timeout=600000 \
    --insert_keeper_fault_injection_probability=0 \
    -q "INSERT INTO quorum_r1 VALUES (1)" > /dev/null 2>&1 &
INSERT_PID=$!

# Wait until the INSERT is actually waiting for quorum.
for _ in {1..300}; do
    count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'")
    [[ "$count" == "1" ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$QUERY_ID' ASYNC FORMAT Null"

# The killed INSERT must finish quickly (well within insert_quorum_timeout = 600s).
finished=0
for _ in {1..150}; do  # up to ~30s
    if ! kill -0 "$INSERT_PID" 2>/dev/null; then
        finished=1
        break
    fi
    sleep 0.2
done
wait "$INSERT_PID" 2>/dev/null

echo "killed insert finished promptly: $finished"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS quorum_r1 SYNC;
DROP TABLE IF EXISTS quorum_r2 SYNC;
"
