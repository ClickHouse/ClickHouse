#!/usr/bin/env bash
# Tags: no-ordinary-database, no-random-merge-tree-settings, no-encrypted-storage, no-replicated-database

# Test for the race condition where a transaction commits before all mutations
# started by that transaction have been applied to all parts.
# This tests the fix for the bug where selectPartsToMutate would throw:
# "Cannot find transaction ... that has started mutation ... that is going to be applied to part ..."

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_txn_mutation_race" 2>/dev/null || true
}

trap cleanup EXIT

cleanup

# Create a MergeTree table with partitioning to prevent automatic merges across partitions
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_txn_mutation_race (key UInt64, value UInt64)
    ENGINE = MergeTree
    PARTITION BY key % 5
    ORDER BY key
"

# Insert data - each insert goes to multiple partitions
$CLICKHOUSE_CLIENT -q "INSERT INTO t_txn_mutation_race SELECT number, 1 FROM numbers(100)"

# Verify we have multiple parts (one per partition)
PARTS_COUNT=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_txn_mutation_race' AND active")
if [ "$PARTS_COUNT" -lt 3 ]; then
    echo "FAILED: Expected at least 3 parts, got $PARTS_COUNT"
    exit 1
fi

# Run a few iterations to trigger the race condition
# Each iteration starts a transaction, issues a mutation, and commits immediately
# The commit happens before the mutation finishes applying to all parts
for i in {1..3}; do
    $CLICKHOUSE_CLIENT -q "
        BEGIN TRANSACTION;
        ALTER TABLE t_txn_mutation_race UPDATE value = value + 1 WHERE 1;
        COMMIT;
    " 2>&1 | grep -v "^$" || true
done

# Wait for all mutations to complete
for _ in {1..60}; do
    PENDING=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_txn_mutation_race' AND is_done = 0")
    if [ "$PENDING" -eq 0 ]; then
        break
    fi
    sleep 0.5
done

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
        SELECT mutation_id, latest_fail_reason
        FROM system.mutations
        WHERE database = currentDatabase()
          AND table = 't_txn_mutation_race'
          AND latest_fail_reason != ''
    "
    exit 1
fi

# Check mutations completed
PENDING=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_txn_mutation_race' AND is_done = 0")
if [ "$PENDING" -gt 0 ]; then
    echo "FAILED: $PENDING mutations still pending"
    exit 1
fi

echo "OK"
