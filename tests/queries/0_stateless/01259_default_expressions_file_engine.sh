#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh


$CLICKHOUSE_CLIENT -q "CREATE DATABASE IF NOT EXISTS test_1259_default;"

# CLICKHOUSE_DATA_PATH is /etc/clickhouse-server/data, so this test will work only in CI.

mkdir $CLICKHOUSE_DATA_PATH/test_1259_default/table_with_file_engine

echo '{"a":1}' > $CLICKHOUSE_DATA_PATH/table_with_file_engine/data.JSONEachRow

$CLICKHOUSE_CLIENT -n -q \
"create table test_1259_default.table_with_file_engine(a int, b int default 7) engine File(JSONEachRow); \
set input_format_defaults_for_omitted_fields = 1; \
select * from test_1259_default.table_with_file_engine;";

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS test_1259_default;"
