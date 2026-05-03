#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel, long

# Coverage test for PR 61610 — the new "drop/replace intent" mechanism
# (`ReplicatedMergeTreeQueue::addDropReplaceIntent`,
#  `removeDropReplaceIntent`, `isIntersectingWithDropReplaceIntent`,
#  `waitForCurrentlyExecutingOpsInRange`) plus the
#  `MergeList::cancelInPartition` call in
#  `StorageReplicatedMergeTree::replacePartitionFrom`.
#
# Without these guards a concurrent mutation can promote a PreActive part
# during REPLACE PARTITION, "resurrecting" old rows. After REPLACE the row
# count for partition 1 must equal the number of rows in `src` (= 10).
# The non-replicated counterpart already exists as
# 03920_replace_partition_mutation_race.sh.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS dst SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS src SYNC"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE dst (p UInt8, x UInt64, y UInt64 DEFAULT 0)
    ENGINE = ReplicatedMergeTree('/clickhouse/test/04201_${CLICKHOUSE_DATABASE}/dst', 'r1')
    PARTITION BY p
    ORDER BY x
    SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0
"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (p UInt8, x UInt64, y UInt64 DEFAULT 0)
    ENGINE = ReplicatedMergeTree('/clickhouse/test/04201_${CLICKHOUSE_DATABASE}/src', 'r1')
    PARTITION BY p
    ORDER BY x
"

# Populate dst with initial data in partition 1 (more rows than src so resurrection shows up)
$CLICKHOUSE_CLIENT -q "INSERT INTO dst SELECT 1, number, 0 FROM numbers(100)"
# Source has a fixed known set of 10 rows for partition 1
$CLICKHOUSE_CLIENT -q "INSERT INTO src SELECT 1, number, 1000 FROM numbers(10)"

ERRORS_FILE=$(mktemp)

function replace_and_check_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "ALTER TABLE dst REPLACE PARTITION id '1' FROM src" 2>/dev/null
        count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM dst WHERE p = 1" 2>/dev/null)
        if [ -n "$count" ] && [ "$count" -gt 10 ]; then
            echo "FAIL: expected <=10 rows after REPLACE PARTITION, got $count" >> "$ERRORS_FILE"
            return
        fi
    done
}

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

$CLICKHOUSE_CLIENT -q "DROP TABLE dst SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE src SYNC"
