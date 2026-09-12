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
DROP TABLE IF EXISTS test_no_partition_age_merge;
DROP TABLE IF EXISTS test_partition_age_merge;

SELECT 'Without min_partition_age_to_force_merge_seconds';

CREATE TABLE test_no_partition_age_merge (d Date, i Int64) ENGINE = MergeTree PARTITION BY d ORDER BY i
SETTINGS merge_selecting_sleep_ms=1000;
INSERT INTO test_no_partition_age_merge VALUES ('2024-01-01', 1);
INSERT INTO test_no_partition_age_merge VALUES ('2024-01-01', 2);
INSERT INTO test_no_partition_age_merge VALUES ('2024-01-01', 3);"

wait_for_number_of_parts 'test_no_partition_age_merge' 1 10

$CLICKHOUSE_CLIENT -mq "
DROP TABLE test_no_partition_age_merge;

SELECT 'With min_partition_age_to_force_merge_seconds';

CREATE TABLE test_partition_age_merge (d Date, i Int64) ENGINE = MergeTree PARTITION BY d ORDER BY i
SETTINGS min_partition_age_to_force_merge_seconds=1, merge_selecting_sleep_ms=1000;
INSERT INTO test_partition_age_merge VALUES ('2024-01-01', 1);
INSERT INTO test_partition_age_merge VALUES ('2024-01-01', 2);
INSERT INTO test_partition_age_merge VALUES ('2024-01-01', 3);"

wait_for_number_of_parts 'test_partition_age_merge' 1 100

$CLICKHOUSE_CLIENT -q "DROP TABLE test_partition_age_merge;"
