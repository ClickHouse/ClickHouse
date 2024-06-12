#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

max_name_length=$(getconf NAME_MAX /)
database_length=$(echo "${CLICKHOUSE_DATABASE}" | awk '{print length}')
suffix_length=$(echo ".sql.detached" | awk '{print length}')
# 2 dots and uuid will take 38 characters
let allowed_name_length=max_name_length-suffix_length-database_length-38

long_table_name=$(openssl rand -base64 $max_name_length | tr -dc A-Za-z | head -c $max_name_length)
allowed_table_name=$(openssl rand -base64 $allowed_name_length | tr -dc A-Za-z | head -c $allowed_name_length)

$CLICKHOUSE_CLIENT -mn --query="CREATE TABLE $long_table_name (id UInt32, long_table_name String) Engine=MergeTree() order by id;" 2>&1 | grep -o -m 1 TOO_LONG_TABLE_NAME
$CLICKHOUSE_CLIENT -mn --query="CREATE TABLE $allowed_table_name (id UInt32, allowed_table_name String) Engine=MergeTree() order by id;"
$CLICKHOUSE_CLIENT -mn --query="DROP TABLE $allowed_table_name;"
