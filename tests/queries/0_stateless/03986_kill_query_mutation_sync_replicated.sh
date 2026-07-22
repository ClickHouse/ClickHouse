#!/usr/bin/env bash
# Tags: replica, zookeeper, no-fasttest, no-sanitizers-lsan, long
# Test that KILL QUERY works for ALTER DELETE with mutations_sync=1 on ReplicatedMergeTree.
# Ref: https://github.com/ClickHouse/ClickHouse/issues/97535

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="kill_query_mutation_sync_${CLICKHOUSE_DATABASE}_$RANDOM"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_kill_mutation
    (
        id UInt64,
        value String
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_kill_mutation', '1')
    ORDER BY id
"

$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_DATABASE}.t_kill_mutation SELECT number, toString(number) FROM numbers(100)"

# Stop merges so the mutation entry is created but never executed by the background pool.
# The mutation stays incomplete forever, so with mutations_sync=1 the ALTER blocks inside
# waitMutationToFinishOnReplicas (the code path fixed by #97589). This makes the test
# deterministic and CPU-independent: it does not rely on a long-running or memory-heavy
# mutation to keep the query alive, and it leaves no in-flight background work to stall
# teardown.
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES ${CLICKHOUSE_DATABASE}.t_kill_mutation"

$CLICKHOUSE_CLIENT --query_id="$query_id" --query "
    ALTER TABLE ${CLICKHOUSE_DATABASE}.t_kill_mutation DELETE WHERE value = 'nonexistent'
    SETTINGS mutations_sync = 1
" >/dev/null 2>&1 &

wait_for_query_to_start "$query_id"

# KILL QUERY must unblock the ALTER waiting on the mutation. The background ALTER client
# exits once the server returns the cancellation error.
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

# Wait for the ALTER client to finish (should exit promptly after the kill).
wait

# Remove the never-executed mutation entry so the table can be dropped cleanly.
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL MUTATION WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_kill_mutation'" >/dev/null 2>&1 || true

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_kill_mutation SYNC"

echo "OK"
