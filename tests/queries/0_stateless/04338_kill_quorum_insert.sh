#!/usr/bin/env bash
# Tags: replica, no-shared-merge-tree, no-replicated-database
# - replica: uses ReplicatedMergeTree
# - no-shared-merge-tree: relies on two explicit replicas + sequential quorum + STOP FETCHES to keep quorum unsatisfied
# - no-replicated-database: creates two explicit replicas (r1, r2) sharing one ZooKeeper path

# Regression for ReplicatedMergeTreeSink::waitForQuorum: a quorum INSERT that is killed must stop waiting for
# quorum promptly and fail with QUERY_WAS_CANCELLED, instead of blocking for the whole insert_quorum_timeout.
# This is the path that originally tripped the stress-test hung-check (a killed insert stuck in waitForQuorum).
# KILL QUERY needs a second connection, so unlike the max_execution_time cases this cannot be a pure SQL test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS quorum_kill_r1 SYNC;
DROP TABLE IF EXISTS quorum_kill_r2 SYNC;
CREATE TABLE quorum_kill_r1 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/quorum_kill', 'r1') ORDER BY x;
CREATE TABLE quorum_kill_r2 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/quorum_kill', 'r2') ORDER BY x;
-- r2 stays active (so the quorum precondition passes) but never fetches the part, so quorum = 2 is never reached.
SYSTEM STOP FETCHES quorum_kill_r2;
"

QUERY_ID="quorum_kill_${CLICKHOUSE_DATABASE}_$$"

# Synchronous quorum INSERT (async_insert = 0 so this query_id is the one waiting for quorum and can be killed).
# insert_quorum_timeout is huge, so without prompt cancellation the killed query would hang for a long time.
$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" -q "
INSERT INTO quorum_kill_r1
SETTINGS insert_quorum = 2, insert_quorum_parallel = 0, insert_quorum_timeout = 6000000, async_insert = 0, insert_keeper_fault_injection_probability = 0
VALUES (1)" > "${CLICKHOUSE_TMP}/quorum_kill_${CLICKHOUSE_DATABASE}.out" 2>&1 &
INSERT_PID=$!

# Wait until the INSERT is actually waiting for quorum.
started=0
for _ in {1..300}; do
    [[ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'")" == "1" ]] && { started=1; break; }
    sleep 0.1
done
echo "insert is waiting for quorum: $started"

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$QUERY_ID' ASYNC FORMAT Null"

# The killed INSERT must terminate promptly (otherwise this test would time out) and report QUERY_WAS_CANCELLED.
wait "$INSERT_PID" 2>/dev/null
echo "killed insert error: $(grep -oE 'QUERY_WAS_CANCELLED' "${CLICKHOUSE_TMP}/quorum_kill_${CLICKHOUSE_DATABASE}.out" | head -1)"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS quorum_kill_r1 SYNC;
DROP TABLE IF EXISTS quorum_kill_r2 SYNC;
"
