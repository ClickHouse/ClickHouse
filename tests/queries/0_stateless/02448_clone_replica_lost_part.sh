#!/usr/bin/env bash
# Tags: long, no-shared-merge-tree
# no-shared-merge-tree: depend on replication queue/fetches

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# disable fault injection; part ids are non-deterministic in case of insert retries
CH_CLIENT="$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability 0"

# Helper: wait for is_lost to reach a specific value for a replica
# Usage: wait_for_lost <replica_number> <expected_value>
wait_for_lost()
{
    local replica=$1
    local expected=$2
    for _ in $(seq 1 30); do
        result=$($CH_CLIENT --query "SELECT value FROM system.zookeeper WHERE path='/test/02448/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rmt/replicas/${replica}' AND name='is_lost'")
        if [ "$result" = "$expected" ]; then
            return
        fi
        sleep 2
    done
}

$CH_CLIENT --query "DROP TABLE IF EXISTS rmt1"
$CH_CLIENT --query "DROP TABLE IF EXISTS rmt2"

$CH_CLIENT --query "
    CREATE TABLE rmt1 (n int) ENGINE=ReplicatedMergeTree('/test/02448/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rmt', '1') ORDER BY tuple()
        SETTINGS min_replicated_logs_to_keep=1, max_replicated_logs_to_keep=2,
        max_cleanup_delay_period=1, cleanup_delay_period=0, cleanup_delay_period_random_add=1,
        cleanup_thread_preferred_points_per_iteration=0, old_parts_lifetime=0, max_parts_to_merge_at_once=4,
        merge_selecting_sleep_ms=1000, max_merge_selecting_sleep_ms=2000"

$CH_CLIENT --query "
    CREATE TABLE rmt2 (n int) ENGINE=ReplicatedMergeTree('/test/02448/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rmt', '2') ORDER BY tuple()
        SETTINGS min_replicated_logs_to_keep=1, max_replicated_logs_to_keep=2,
        max_cleanup_delay_period=1, cleanup_delay_period=0, cleanup_delay_period_random_add=1,
        cleanup_thread_preferred_points_per_iteration=0, old_parts_lifetime=0, max_parts_to_merge_at_once=4,
        merge_selecting_sleep_ms=1000, max_merge_selecting_sleep_ms=2000"

# insert part only on one replica
$CH_CLIENT --query "SYSTEM STOP REPLICATED SENDS rmt1"
$CH_CLIENT --query "INSERT INTO rmt1 VALUES (1)"
$CH_CLIENT --query "DETACH TABLE rmt1"      # make replica inactive
$CH_CLIENT --query "SYSTEM START REPLICATED SENDS rmt1"

# trigger log rotation, rmt1 will be lost
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (2)"
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (3)"
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (4)"
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (5)"
# check that entry was not removed from the queue (part is not lost)
$CH_CLIENT --receive_timeout 5 --query "SYSTEM SYNC REPLICA rmt2" 2>/dev/null

$CH_CLIENT --query "SELECT 1, arraySort(groupArray(n)) FROM rmt2"

# rmt1 will mimic rmt2
$CH_CLIENT --query "ATTACH TABLE rmt1"
$CH_CLIENT --query "SYSTEM SYNC REPLICA rmt1"
$CH_CLIENT --query "SYSTEM SYNC REPLICA rmt2"

# check that no parts are lost
$CH_CLIENT --query "SELECT 2, arraySort(groupArray(n)) FROM rmt1"
$CH_CLIENT --query "SELECT 3, arraySort(groupArray(n)) FROM rmt2"


$CH_CLIENT --query "TRUNCATE TABLE rmt1"
$CH_CLIENT --query "TRUNCATE TABLE rmt2"


# insert parts only on one replica and merge them
$CH_CLIENT --query "SYSTEM STOP REPLICATED SENDS rmt2"
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (1)"
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (2)"
$CH_CLIENT --query "SYSTEM SYNC REPLICA rmt2"
$CH_CLIENT --query "OPTIMIZE TABLE rmt2 FINAL"
$CH_CLIENT --query "SYSTEM SYNC REPLICA rmt2"
# give it a chance to remove source parts
sleep 2  # increases probability of reproducing the issue
$CH_CLIENT --query "DETACH TABLE rmt2"
$CH_CLIENT --query "SYSTEM START REPLICATED SENDS rmt2"


# trigger log rotation, rmt2 will be lost
$CH_CLIENT --query "INSERT INTO rmt1 VALUES (3)"
$CH_CLIENT --query "INSERT INTO rmt1 VALUES (4)"
$CH_CLIENT --query "INSERT INTO rmt1 VALUES (5)"
# check that entry was not removed from the queue (part is not lost)
$CH_CLIENT --receive_timeout 5 --query "SYSTEM SYNC REPLICA rmt1" 2>/dev/null

$CH_CLIENT --query "SELECT 4, arraySort(groupArray(n)) FROM rmt1"

# rmt2 will mimic rmt1
$CH_CLIENT --query "SYSTEM STOP FETCHES rmt1"
$CH_CLIENT --query "ATTACH TABLE rmt2"
$CH_CLIENT --query "SYSTEM SYNC REPLICA rmt2"
# give rmt2 a chance to remove merged part (but it should not do it)
sleep 2  # increases probability of reproducing the issue
$CH_CLIENT --query "SYSTEM START FETCHES rmt1"
$CH_CLIENT --query "SYSTEM SYNC REPLICA rmt1"

# check that no parts are lost
$CH_CLIENT --query "SELECT 5, arraySort(groupArray(n)) FROM rmt1"
$CH_CLIENT --query "SELECT 6, arraySort(groupArray(n)) FROM rmt2"


# insert part only on one replica
$CH_CLIENT --query "SYSTEM STOP REPLICATED SENDS rmt1"
$CH_CLIENT --query "INSERT INTO rmt1 VALUES (123)"
$CH_CLIENT --query "ALTER TABLE rmt1 UPDATE n=10 WHERE n=123 SETTINGS mutations_sync=1"
# give it a chance to remove source part
sleep 2  # increases probability of reproducing the issue
$CH_CLIENT --query "DETACH TABLE rmt1"      # make replica inactive
$CH_CLIENT --query "SYSTEM START REPLICATED SENDS rmt1"

# trigger log rotation, rmt1 will be lost
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (20)"
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (30)"
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (40)"
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (50)"
# check that entry was not removed from the queue (part is not lost)
$CH_CLIENT --receive_timeout 5 --query "SYSTEM SYNC REPLICA rmt2" 2>/dev/null

$CH_CLIENT --query "SELECT 7, arraySort(groupArray(n)) FROM rmt2"

# rmt1 will mimic rmt2
$CH_CLIENT --query "SYSTEM STOP FETCHES rmt2"
$CH_CLIENT --query "ATTACH TABLE rmt1"
$CH_CLIENT --query "SYSTEM SYNC REPLICA rmt1"
# give rmt1 a chance to remove mutated part (but it should not do it)
sleep 2  # increases probability of reproducing the issue
$CH_CLIENT --query "SYSTEM START FETCHES rmt2"
$CH_CLIENT --query "SYSTEM SYNC REPLICA rmt2"

# check that no parts are lost
$CH_CLIENT --query "SELECT 8, arraySort(groupArray(n)) FROM rmt1"
$CH_CLIENT --query "SELECT 9, arraySort(groupArray(n)) FROM rmt2"

# avoid arbitrary merges after inserting
$CH_CLIENT --query "OPTIMIZE TABLE rmt2 FINAL"
# insert parts (all_18_18_0, all_19_19_0) on both replicas (will be deduplicated, but it does not matter)
$CH_CLIENT --query "INSERT INTO rmt1 VALUES (100)"
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (100)"
$CH_CLIENT --query "INSERT INTO rmt1 VALUES (200)"
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (200)"

# otherwise we can get exception on drop part
$CH_CLIENT --query "SYSTEM SYNC REPLICA rmt2"
$CH_CLIENT --query "SYSTEM SYNC REPLICA rmt1"

$CH_CLIENT --query "DETACH TABLE rmt1"

# create a gap in block numbers by dropping part
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (300)"
$CH_CLIENT --query "ALTER TABLE rmt2 DROP PART 'all_19_19_0'"   # remove 200
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (400)"
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (500)"
$CH_CLIENT --query "INSERT INTO rmt2 VALUES (600)"
$CH_CLIENT --query "SYSTEM SYNC REPLICA rmt2"
# merge through gap
$CH_CLIENT --query "OPTIMIZE TABLE rmt2"

# wait for rmt1 to be marked as lost (is_lost transitions from 0 to 1)
wait_for_lost 1 1

# rmt1 will mimic rmt2, but will not be able to fetch parts for a while
$CH_CLIENT --query "SYSTEM STOP REPLICATED SENDS rmt2"
$CH_CLIENT --query "ATTACH TABLE rmt1"

# wait for clone process to complete (is_lost transitions from 1 to 0 after parts are removed from working set)
wait_for_lost 1 0

# rmt1 should not show the value (200) from dropped part
$CH_CLIENT --query "SELECT throwIf(n = 200) FROM rmt1 FORMAT Null"
$CH_CLIENT --query "SELECT 11, arraySort(groupArray(n)) FROM rmt2"

$CH_CLIENT --query "SYSTEM START REPLICATED SENDS rmt2"
$CH_CLIENT --query "SYSTEM SYNC REPLICA rmt1"
$CH_CLIENT --query "SELECT 12, arraySort(groupArray(n)) FROM rmt1"

$CH_CLIENT --query "DROP TABLE rmt1"
$CH_CLIENT --query "DROP TABLE rmt2"
