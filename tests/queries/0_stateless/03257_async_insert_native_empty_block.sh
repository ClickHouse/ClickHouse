#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS json_square_brackets;
    CREATE TABLE json_square_brackets (id UInt32, name String) ENGINE = MergeTree ORDER BY tuple()
"

MY_CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --async_insert 1 --wait_for_async_insert 1"

echo '[{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}]' | $MY_CLICKHOUSE_CLIENT -q "INSERT INTO json_square_brackets FORMAT JSONEachRow"

echo '[{"id": 3}, {"id": 4}, {"id": 5}]' | $MY_CLICKHOUSE_CLIENT -q "INSERT INTO json_square_brackets FORMAT JSONEachRow"

echo '[]' | $MY_CLICKHOUSE_CLIENT -q "INSERT INTO json_square_brackets FORMAT JSONEachRow"

echo '' | $MY_CLICKHOUSE_CLIENT -q "INSERT INTO json_square_brackets FORMAT JSONEachRow"

$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS;
    SELECT * FROM json_square_brackets ORDER BY id;
    SELECT status, data_kind, rows FROM system.asynchronous_insert_log WHERE database = currentDatabase() AND table = 'json_square_brackets' ORDER BY event_time_microseconds;
    DROP TABLE json_square_brackets;
"
