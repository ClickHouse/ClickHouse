#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for a race condition between REPLACE PARTITION and background mutations.
#
# Three related bugs were fixed:
# 1. `merges_blocker` ActionLock in `replacePartitionFrom` was scoped inside the if/else block
#    and destroyed before the actual partition replacement. A background mutation could start on
#    parts in the target partition after the blocker was released.
# 2. The mutation task committed its result in two separate `data_parts_lock` acquisitions:
#    first `renameTempPartAndReplace` (adds the result as PreActive), then `transaction.commit`
#    (promotes to Active). If REPLACE PARTITION ran between these two steps, its
#    `removePartsInRangeFromWorkingSet` only removed Active parts and missed the PreActive
#    mutation result. After REPLACE released the lock, the mutation's commit promoted the
#    PreActive part to Active, "resurrecting" old data.
# 3. `selectPartsToMutate` did not check per-partition merge blockers. Even after
#    `stopMergesAndWaitForPartition` set the partition blocker and drained
#    `currently_merging_mutating_parts`, a new mutation could be selected because the
#    `isCancelledForPartition` check in `scheduleDataProcessingJob` ran outside
#    `currently_processing_in_background_mutex`, creating a window for the scheduling
#    thread to pick the mutation between the blocker being set and the check.
#
# https://github.com/ClickHouse/ClickHouse/issues/73783

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS src"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE dst (p UInt8, x UInt64, y UInt64 DEFAULT 0)
    ENGINE = MergeTree
    PARTITION BY p
    ORDER BY x
    SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0
"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (p UInt8, x UInt64, y UInt64 DEFAULT 0)
    ENGINE = MergeTree
    PARTITION BY p
    ORDER BY x
"

# Populate dst with initial data in partition 1 (more rows than src to make resurrection detectable)
$CLICKHOUSE_CLIENT -q "INSERT INTO dst SELECT 1, number, 0 FROM numbers(100)"

# Source has a fixed known set of 10 rows for partition 1
$CLICKHOUSE_CLIENT -q "INSERT INTO src SELECT 1, number, 1000 FROM numbers(10)"

ERRORS_FILE=$(mktemp)

# After each REPLACE, partition 1 should have exactly 10 rows (from src).
# If a background mutation "resurrects" old parts, count will be > 10.
function replace_and_check_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "ALTER TABLE dst REPLACE PARTITION id '1' FROM src" 2>/dev/null
        count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM dst WHERE p = 1" 2>/dev/null)
        if [ -n "$count" ] && [ "$count" -gt 10 ]; then
            echo "FAIL: expected 10 rows after REPLACE PARTITION, got $count" >> "$ERRORS_FILE"
            return
        fi
    done
}

# Continuously submit mutations that touch the same partition.
# These mutations only UPDATE values (don't add rows), so they shouldn't affect row count.
function mutation_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "ALTER TABLE dst UPDATE y = y + 1 WHERE p = 1" 2>/dev/null
        sleep 0.0$RANDOM
    done
}

TIMEOUT=10

replace_and_check_thread &
mutation_thread &
mutation_thread &

wait

# Wait for any remaining background mutations to finish
counter=0
while [ $counter -lt 60 ]; do
    undone=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'dst' AND is_done = 0" 2>/dev/null)
    if [ -n "$undone" ] && [ "$undone" -eq 0 ]; then
        break
    fi
    sleep 1
    counter=$((counter + 1))
done

if [ -s "$ERRORS_FILE" ]; then
    cat "$ERRORS_FILE"
else
    echo "OK"
fi

rm -f "$ERRORS_FILE"

$CLICKHOUSE_CLIENT -q "DROP TABLE dst"
$CLICKHOUSE_CLIENT -q "DROP TABLE src"
