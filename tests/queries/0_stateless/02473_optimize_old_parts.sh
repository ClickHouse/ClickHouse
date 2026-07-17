#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Wait for number of parts in table $1 to become $2.
# Print the changed value. If no changes for $3 seconds, prints initial value.
# $4 (optional, non-empty): nudge the merge selector every second.
wait_for_number_of_parts() {
    for _ in `seq $3`
    do
        # Nudge only in the min_age_to_force_merge_seconds branches. For
        # non-replicated MergeTree the merge cadence is the background-pool
        # no-work backoff (up to 600s), which merge_selecting_sleep_ms does not
        # affect, so rescheduling selection bounds the wait for a forced merge.
        # It only schedules selection, so min_age/byte-limit rules still decide
        # what merges. The control case must NOT nudge: SYSTEM START MERGES also
        # runs the ordinary merge selector, which could merge the small parts and
        # break the "no force-merge within 10s" assertion.
        if [ -n "$4" ]; then
            $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES $1" 2>/dev/null
        fi
        sleep 1
        res=`$CLICKHOUSE_CLIENT -q "SELECT count(*) FROM system.parts WHERE database = currentDatabase() AND table='$1' AND active"`
        if [ "$res" -eq "$2" ]
        then
            echo "$res"
            return
        fi
    done
    echo "$res"
}

$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS test_without_merge;
DROP TABLE IF EXISTS test_with_merge;
DROP TABLE IF EXISTS test_with_merge_limit;

SELECT 'Without merge';

CREATE TABLE test_without_merge (i Int64) ENGINE = MergeTree ORDER BY i SETTINGS merge_selecting_sleep_ms=1000;
INSERT INTO test_without_merge SELECT 1;
INSERT INTO test_without_merge SELECT 2;
INSERT INTO test_without_merge SELECT 3;"

wait_for_number_of_parts 'test_without_merge' 1 10

$CLICKHOUSE_CLIENT -mq "
DROP TABLE test_without_merge;

SELECT 'With merge any part range';

CREATE TABLE test_with_merge (i Int64) ENGINE = MergeTree ORDER BY i
SETTINGS min_age_to_force_merge_seconds=1, merge_selecting_sleep_ms=1000, min_age_to_force_merge_on_partition_only=false;
INSERT INTO test_with_merge SELECT 1;
INSERT INTO test_with_merge SELECT 2;
INSERT INTO test_with_merge SELECT 3;"

wait_for_number_of_parts 'test_with_merge' 1 100 nudge

$CLICKHOUSE_CLIENT -mq "
DROP TABLE test_with_merge;

SELECT 'With merge partition only';

CREATE TABLE test_with_merge (i Int64) ENGINE = MergeTree ORDER BY i PARTITION BY i
SETTINGS min_age_to_force_merge_seconds=1, merge_selecting_sleep_ms=1000, min_age_to_force_merge_on_partition_only=true,
-- Force-merging an entire partition is skipped when the shared background pool is busy with
-- unrelated tasks (see number_of_free_entries_in_pool_to_execute_optimize_entire_partition).
-- Pin it low so this test does not depend on how busy the pool happens to be.
number_of_free_entries_in_pool_to_execute_optimize_entire_partition=1;
INSERT INTO test_with_merge SELECT 1;
INSERT INTO test_with_merge SELECT 2;
INSERT INTO test_with_merge SELECT 2 SETTINGS insert_deduplicate = 0;"

wait_for_number_of_parts 'test_with_merge' 2 100 nudge

$CLICKHOUSE_CLIENT -mq "
SELECT sleepEachRow(1) FROM numbers(9) SETTINGS function_sleep_max_microseconds_per_block = 10000000 FORMAT Null; -- Sleep for 9 seconds and verify that we keep the old part because it's the only one
SELECT (now() - modification_time) > 5 FROM system.parts WHERE database = currentDatabase() AND table='test_with_merge' AND active;

DROP TABLE test_with_merge;"

# Partition 2 will ignore max_bytes_to_merge_at_max_space_in_pool
$CLICKHOUSE_CLIENT -mq "
SELECT 'With merge partition only and disable limit';

CREATE TABLE test_with_merge_limit (i Int64) ENGINE = MergeTree ORDER BY i PARTITION BY i
SETTINGS min_age_to_force_merge_seconds=1, merge_selecting_sleep_ms=1000, min_age_to_force_merge_on_partition_only=true, enable_max_bytes_limit_for_min_age_to_force_merge=false, max_bytes_to_merge_at_max_space_in_pool=1,
number_of_free_entries_in_pool_to_execute_optimize_entire_partition=1;
INSERT INTO test_with_merge_limit SELECT 1;
INSERT INTO test_with_merge_limit SELECT 2;
INSERT INTO test_with_merge_limit SELECT 2 SETTINGS insert_deduplicate = 0;"

wait_for_number_of_parts 'test_with_merge_limit' 2 100 nudge

# Partition 2 will limit by max_bytes_to_merge_at_max_space_in_pool
$CLICKHOUSE_CLIENT -mq "
DROP TABLE test_with_merge_limit;

SELECT 'With merge partition only and enable limit';

CREATE TABLE test_with_merge_limit (i Int64) ENGINE = MergeTree ORDER BY i PARTITION BY i
SETTINGS min_age_to_force_merge_seconds=1, merge_selecting_sleep_ms=1000, min_age_to_force_merge_on_partition_only=true, enable_max_bytes_limit_for_min_age_to_force_merge=true, max_bytes_to_merge_at_max_space_in_pool=1,
number_of_free_entries_in_pool_to_execute_optimize_entire_partition=1;
INSERT INTO test_with_merge_limit SELECT 1;
INSERT INTO test_with_merge_limit SELECT 2;
INSERT INTO test_with_merge_limit SELECT 2 SETTINGS insert_deduplicate = 0;"

wait_for_number_of_parts 'test_with_merge_limit' 3 100 nudge

$CLICKHOUSE_CLIENT -mq "
DROP TABLE test_with_merge_limit;"