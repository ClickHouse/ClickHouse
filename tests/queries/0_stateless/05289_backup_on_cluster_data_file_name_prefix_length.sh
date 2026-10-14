#!/usr/bin/env bash
# A `BACKUP ... ON CLUSTER` with `data_file_name_generator = 'checksum'` and `data_file_name_prefix_length`
# set in the query must be restorable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt64, s String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT number, toString(number) FROM numbers(1000);
"

for prefix_length in 1 0; do
    backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_${prefix_length}')"
    $CLICKHOUSE_CLIENT --query "
        BACKUP TABLE ${CLICKHOUSE_DATABASE}.t ON CLUSTER test_shard_localhost TO $backup_name
        SETTINGS data_file_name_generator = 'checksum', data_file_name_prefix_length = $prefix_length" | grep -o "BACKUP_CREATED"
    $CLICKHOUSE_CLIENT --query "
        RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_$prefix_length FROM $backup_name" | grep -o "RESTORED"
    $CLICKHOUSE_CLIENT --query "SELECT $prefix_length, count(), sum(x), sum(length(s)) FROM ${CLICKHOUSE_DATABASE}.t_$prefix_length"
done
