#!/usr/bin/env bash
# Tags: long, no-ordinary-database

# Test for the race condition where a transaction commits before all mutations
# started by that transaction have been applied to all parts.
# This tests the fix for the bug where selectPartsToMutate would throw:
# "Cannot find transaction ... that has started mutation ... that is going to be applied to part ..."

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_txn_mutation_race" 2>/dev/null || true
}

trap cleanup EXIT

cleanup

# Create a MergeTree table (not replicated, as the bug is in StorageMergeTree)
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_txn_mutation_race (key UInt64, value UInt64)
    ENGINE = MergeTree
    ORDER BY key
    SETTINGS old_parts_lifetime = 0
"

# Insert multiple parts so mutations take some time to apply
for i in {1..10}; do
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_txn_mutation_race SELECT number, $i FROM numbers(1000)"
done

# Verify we have multiple parts
PARTS_COUNT=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_txn_mutation_race' AND active")
if [ "$PARTS_COUNT" -lt 5 ]; then
    echo "FAILED: Expected at least 5 parts, got $PARTS_COUNT"
    exit 1
fi

# Run multiple iterations to increase chance of hitting the race condition
# Each iteration:
# 1. Starts a transaction
# 2. Starts a mutation (UPDATE)
# 3. Commits the transaction immediately (without waiting for mutation to complete)
# The bug would cause a LOGICAL_ERROR when the background mutation job tries to
# apply the mutation after the transaction has committed.

TIMEOUT=$((SECONDS + 30))
ITERATIONS=0

while [ $SECONDS -lt "$TIMEOUT" ] && [ $ITERATIONS -lt 20 ]; do
    ITERATIONS=$((ITERATIONS + 1))

    # Start transaction, mutation, and commit without waiting for mutation
    # The transaction commits while mutation is still being applied in background
    $CLICKHOUSE_CLIENT -q "
        BEGIN TRANSACTION;
        ALTER TABLE t_txn_mutation_race UPDATE value = value + 1 WHERE 1;
        COMMIT;
    " 2>&1 | grep -v "^$" || true
done

# Wait for all mutations to complete
wait_for_all_mutations "t_txn_mutation_race"

# Check that all mutations succeeded (no failed mutations)
FAILED_MUTATIONS=$($CLICKHOUSE_CLIENT -q "
    SELECT count()
    FROM system.mutations
    WHERE database = currentDatabase()
      AND table = 't_txn_mutation_race'
      AND latest_fail_reason != ''
")

if [ "$FAILED_MUTATIONS" -gt 0 ]; then
    echo "FAILED: Found $FAILED_MUTATIONS failed mutations"
    $CLICKHOUSE_CLIENT -q "
        SELECT mutation_id, latest_fail_reason, latest_fail_time
        FROM system.mutations
        WHERE database = currentDatabase()
          AND table = 't_txn_mutation_race'
          AND latest_fail_reason != ''
    "
    exit 1
fi

echo "OK: All mutations completed successfully"

# Verify data integrity - all rows should have the same value (initial + number of iterations)
DISTINCT_VALUES=$($CLICKHOUSE_CLIENT -q "SELECT uniqExact(value) FROM t_txn_mutation_race")
if [ "$DISTINCT_VALUES" -ne 1 ]; then
    echo "FAILED: Expected all rows to have the same value, but found $DISTINCT_VALUES distinct values"
    $CLICKHOUSE_CLIENT -q "SELECT value, count() FROM t_txn_mutation_race GROUP BY value ORDER BY value"
    exit 1
fi

echo "OK: Data integrity verified"

# Table is dropped by cleanup trap
