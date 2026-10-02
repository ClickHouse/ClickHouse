#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS 02784_async_table_with_dedup"

$CLICKHOUSE_CLIENT -q "CREATE TABLE 02784_async_table_with_dedup (a Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/02784_async_table_with_dedup', 'r1') ORDER BY a"

CLICKHOUSE_CLIENT_WITH_LOG=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=trace/g')

function insert_with_log_check() {
    $CLICKHOUSE_CLIENT_WITH_LOG --async-insert=1 --async_insert_deduplicate=1 --wait_for_async_insert=1 -q "$1" 2>&1 | grep -Fc "Setting async_insert=1, but INSERT query will be executed synchronously"
}

insert_with_log_check "INSERT INTO 02784_async_table_with_dedup VALUES (1), (2)"
insert_with_log_check "INSERT INTO 02784_async_table_with_dedup SELECT number as a FROM system.numbers LIMIT 10 OFFSET 3"

DATA_FILE=test_02784_async_$CLICKHOUSE_TEST_UNIQUE_NAME.csv
echo -e '13\n14' > $DATA_FILE

insert_with_log_check "INSERT INTO 02784_async_table_with_dedup FROM INFILE '$DATA_FILE' FORMAT CSV"

$CLICKHOUSE_CLIENT -q "SELECT a FROM 02784_async_table_with_dedup ORDER BY a"

$CLICKHOUSE_CLIENT -q "DROP TABLE 02784_async_table_with_dedup"

rm $DATA_FILE