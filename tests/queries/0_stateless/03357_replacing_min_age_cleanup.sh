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
SET insert_deduplicate = 0;

DROP TABLE IF EXISTS replacing;

CREATE TABLE replacing (key int, value int, version int, deleted UInt8) ENGINE = ReplacingMergeTree(version, deleted) ORDER BY key;

INSERT INTO replacing VALUES (1, 1, 1, 0), (1, 1, 2, 1);

-- Should show single deleted row (the two inserted rows were merged since optimize_on_insert is enabled by default)
SELECT * FROM replacing;

-- Should show nothing, FINAL does not return deleted rows
SELECT * FROM replacing FINAL;

-- Turn on automatic cleanup merge
ALTER TABLE replacing MODIFY SETTING
    allow_experimental_replacing_merge_with_cleanup = true,
    enable_replacing_merge_with_cleanup_for_min_age_to_force_merge = true,
    min_age_to_force_merge_on_partition_only = true,
    min_age_to_force_merge_seconds = 1,
    merge_selecting_sleep_ms = 1000;

-- Do inserts separately to create two parts to merge
INSERT INTO replacing VALUES (1, 1, 1, 0);
INSERT INTO replacing VALUES (1, 1, 2, 1);"

wait_for_number_of_parts 'replacing' 0 10

$CLICKHOUSE_CLIENT -mq "
DROP TABLE replacing;"
