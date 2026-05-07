#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest

# Regression test for #100466: Keeper client should NOT kill a session under
# load when the server is making progress (responding to other requests).
#
# Before the fix, a burst of concurrent reads through system.zookeeper would
# cause the oldest pipelined request to exceed operation_timeout_ms (10s in CI),
# triggering finalize() and killing the entire session.
#
# The test floods system.zookeeper reads via clickhouse-benchmark, then checks
# that the SAME session is still alive by comparing client_id before and after.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_keeper_progress;
    CREATE TABLE t_keeper_progress (key UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_keeper_progress', 'r1')
    ORDER BY key;
"

ZK_PATH="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_keeper_progress"

# Snapshot session identity (client_id) BEFORE the flood.
CLIENT_ID_BEFORE=$($CLICKHOUSE_CLIENT -q "
    SELECT client_id FROM system.zookeeper_connection
    WHERE name = 'default' LIMIT 1
")

if [ -z "$CLIENT_ID_BEFORE" ]; then
    echo "FAIL: could not read client_id before benchmark"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_keeper_progress"
    exit 1
fi

# Flood ZooKeeper with concurrent reads to generate pipeline backlog.
# The timelimit (15s) generates deep pipelining that would trigger the old
# bug (operation_timeout_ms=10s). With the unified session_timeout_ms (30s)
# the test still validates that no false-positive session kill occurs.
# 50 concurrent connections generate deep pipelining.
BENCHMARK_EXIT_CODE=0
BENCHMARK_OUTPUT=$(echo "SELECT count() FROM system.zookeeper WHERE path = '$ZK_PATH' FORMAT Null" | \
    ${CLICKHOUSE_BENCHMARK} --concurrency 50 --iterations 200000 --timelimit 15 2>&1) || BENCHMARK_EXIT_CODE=$?

if [ "$BENCHMARK_EXIT_CODE" -ne 0 ]; then
    echo "FAIL: benchmark exited with code $BENCHMARK_EXIT_CODE"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_keeper_progress"
    exit 1
fi

if ! echo "$BENCHMARK_OUTPUT" | grep -q "executed"; then
    echo "FAIL: benchmark did not execute"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_keeper_progress"
    exit 1
fi

# Snapshot session identity AFTER the flood.
CLIENT_ID_AFTER=$($CLICKHOUSE_CLIENT -q "
    SELECT client_id FROM system.zookeeper_connection
    WHERE name = 'default' LIMIT 1
")

if [ "$CLIENT_ID_BEFORE" = "$CLIENT_ID_AFTER" ]; then
    echo "OK"
else
    echo "FAIL: session killed and reconnected (client_id changed from $CLIENT_ID_BEFORE to $CLIENT_ID_AFTER)"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_keeper_progress"
