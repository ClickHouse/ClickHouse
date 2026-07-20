#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Wait for number of parts in table $1 to become $2.
# Print the changed value. If no changes for $3 seconds, prints initial value.
wait_for_number_of_parts() {
    for _ in `seq $3`
    do
        res=`$CLICKHOUSE_CLIENT -q "SELECT count(*) FROM system.parts WHERE database = currentDatabase() AND table='$1' AND active"`
        if [ "$res" -eq "$2" ]
        then
            echo "$res"
            return
        fi
        sleep 1
    done
    echo "$res (FAIL)"
}

$CLICKHOUSE_CLIENT -mq "
SET alter_sync = 2;

DROP TABLE IF EXISTS replacing;

CREATE TABLE replacing (key int, value int, version int, deleted UInt8) ENGINE = ReplacingMergeTree(version, deleted) ORDER BY key SETTINGS merge_tree_clear_old_parts_interval_seconds = 1;

INSERT INTO replacing VALUES (1, 1, 1, 0), (1, 1, 2, 1);

-- Should show single deleted row (the two inserted rows were merged since optimize_on_insert is enabled by default)
SELECT * FROM replacing;

-- Should show nothing, FINAL does not return deleted rows
SELECT * FROM replacing FINAL;

-- Just FINAL should not do anything
OPTIMIZE TABLE replacing FINAL;

-- Should still show a single row
SELECT * FROM replacing;

OPTIMIZE TABLE replacing FINAL CLEANUP; -- { serverError SUPPORT_IS_DISABLED }

ALTER TABLE replacing MODIFY SETTING allow_experimental_replacing_merge_with_cleanup = true;

OPTIMIZE TABLE replacing FINAL CLEANUP;
"

# OPTIMIZE FINAL CLEANUP will replace the part with empty, but only cleaner thread will clean it, so we still need to wait
wait_for_number_of_parts 'replacing' 0 30

$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS replacing2;

CREATE TABLE replacing2 (key int, value int, version int, deleted UInt8) ENGINE = ReplacingMergeTree(version, deleted) ORDER BY key
SETTINGS allow_experimental_replacing_merge_with_cleanup = true,
    enable_replacing_merge_with_cleanup_for_min_age_to_force_merge = true,
    merge_tree_clear_old_parts_interval_seconds = 1,
    number_of_free_entries_in_pool_to_execute_optimize_entire_partition = 1,
    min_age_to_force_merge_on_partition_only = true,
    min_age_to_force_merge_seconds = 1,
    merge_selecting_sleep_ms = 1000,
    max_merge_selecting_sleep_ms = 1000;

-- Do inserts separately to create two parts to merge
INSERT INTO replacing2 VALUES (1, 1, 1, 0);
INSERT INTO replacing2 VALUES (1, 1, 2, 1);"

wait_for_number_of_parts 'replacing2' 0 30

$CLICKHOUSE_CLIENT -mq "
SET alter_sync = 2;

DROP TABLE IF EXISTS t03357_replacing_replicated;

CREATE TABLE t03357_replacing_replicated (key int, value int, version int, deleted UInt8)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{database}/t03357_replacing_replicated', 'node', version, deleted)
ORDER BY key
SETTINGS
    cleanup_delay_period = 1,
    max_cleanup_delay_period = 1,
    cleanup_delay_period_random_add = 0;

INSERT INTO t03357_replacing_replicated VALUES (1, 1, 1, 0), (1, 1, 2, 1);

-- Should show single deleted row (the two inserted rows were merged since optimize_on_insert is enabled by default)
SELECT * FROM t03357_replacing_replicated;

-- Should show nothing, FINAL does not return deleted rows
SELECT * FROM t03357_replacing_replicated FINAL;

-- Just FINAL should not do anything
OPTIMIZE TABLE t03357_replacing_replicated FINAL;

-- Should still show a single row
SELECT * FROM t03357_replacing_replicated;

OPTIMIZE TABLE t03357_replacing_replicated FINAL CLEANUP; -- { serverError SUPPORT_IS_DISABLED }

ALTER TABLE t03357_replacing_replicated MODIFY SETTING allow_experimental_replacing_merge_with_cleanup = true;

OPTIMIZE TABLE t03357_replacing_replicated FINAL CLEANUP;
"

# OPTIMIZE FINAL CLEANUP will replace the part with empty, but only cleaner thread will clean it, so we still need to wait
wait_for_number_of_parts 't03357_replacing_replicated' 0 30

$CLICKHOUSE_CLIENT -mq "
DROP TABLE IF EXISTS t03357_replacing_replicated2;

CREATE TABLE t03357_replacing_replicated2 (key int, value int, version int, deleted UInt8) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{database}/t03357_replacing_replicated2', 'node', version, deleted) ORDER BY key
SETTINGS allow_experimental_replacing_merge_with_cleanup = true,
    enable_replacing_merge_with_cleanup_for_min_age_to_force_merge = true,
    number_of_free_entries_in_pool_to_execute_optimize_entire_partition = 1,
    min_age_to_force_merge_on_partition_only = true,
    min_age_to_force_merge_seconds = 1,
    merge_selecting_sleep_ms = 1000,
    max_merge_selecting_sleep_ms = 1000,
    cleanup_delay_period_random_add = 0,
    cleanup_delay_period = 1,
    max_cleanup_delay_period = 1;

-- Do inserts separately to create two parts to merge
INSERT INTO t03357_replacing_replicated2 VALUES (1, 1, 1, 0);
INSERT INTO t03357_replacing_replicated2 VALUES (1, 1, 2, 1);
"

wait_for_number_of_parts 't03357_replacing_replicated2' 0 30

$CLICKHOUSE_CLIENT -mq "
DROP TABLE replacing;
DROP TABLE replacing2;
DROP TABLE t03357_replacing_replicated;
DROP TABLE t03357_replacing_replicated2;
"
