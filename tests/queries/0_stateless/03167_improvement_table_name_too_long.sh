#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

allowed_name_length=$($CLICKHOUSE_CLIENT -mn --query="SELECT getMaxTableNameLengthForDatabase('$CLICKHOUSE_DATABASE')")
let excess_length=allowed_name_length+1

long_table_name=$(openssl rand -base64 $excess_length | tr -dc A-Za-z | head -c $excess_length)
allowed_table_name=$(openssl rand -base64 $allowed_name_length | tr -dc A-Za-z | head -c $allowed_name_length)

$CLICKHOUSE_CLIENT -mn --query="CREATE TABLE $long_table_name (id UInt32, long_table_name String) Engine=MergeTree() order by id;" 2>&1 | grep -o -m "ARGUMENT_OUT_OF_BOUND"
$CLICKHOUSE_CLIENT -mn --query="CREATE TABLE $allowed_table_name (id UInt32, allowed_table_name String) Engine=MergeTree() order by id;"
$CLICKHOUSE_CLIENT -mn --query="DROP TABLE $allowed_table_name;"
