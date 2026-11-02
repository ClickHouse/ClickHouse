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

# This ALTER DELETE uses sleepEachRow to make the mutation take a very long time
# (100 rows * 3 seconds = ~300 seconds) without consuming significant memory.
# The condition is always false (sleepEachRow returns 0), so no rows are deleted,
# but the mutation still scans every row. With mutations_sync=1 the query blocks
# waiting for the mutation to complete.
$CLICKHOUSE_CLIENT --query_id="$query_id" --query "
    ALTER TABLE ${CLICKHOUSE_DATABASE}.t_kill_mutation DELETE WHERE sleepEachRow(3) = 1
    SETTINGS mutations_sync = 1, allow_nondeterministic_mutations = 1
" >/dev/null 2>&1 &

wait_for_query_to_start "$query_id"

# Use async KILL (without SYNC) to avoid blocking if propagation is slow.
# The background ALTER client will exit when the server sends back the cancellation error.
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

# Kill the mutation immediately so it stops consuming resources in the background.
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL MUTATION WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_kill_mutation'" >/dev/null 2>&1 || true

# Wait for the ALTER client to finish (should exit promptly after the kill).
wait

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_kill_mutation SYNC"

echo "OK"
