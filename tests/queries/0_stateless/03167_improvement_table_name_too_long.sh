#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -mn --query="DROP TABLE IF EXISTS for_get_metadata_path;"
$CLICKHOUSE_CLIENT -mn --query="CREATE TABLE for_get_metadata_path(id UInt32, v String) Engine=MergeTree() order by id;"
sql_path=$($CLICKHOUSE_CLIENT -mn --query="select metadata_path from system.tables where name = 'for_get_metadata_path' and database=currentDatabase()")
path=$(echo $sql_path | awk -F'store' '{print $1}')
metadata_path=$path"metadata/${CLICKHOUSE_DATABASE}"
metadata_dropped_path=$path"metadata_dropped"

max_create_length=$(getconf NAME_MAX $metadata_path)
max_drop_length=$(getconf NAME_MAX $metadata_dropped_path)
suffix_length=$(echo ".sql.detached" | awk '{print length}')
database_length=$(echo "${CLICKHOUSE_DATABASE}" | awk '{print length}')

let max_to_create=max_create_length-suffix_length
let max_to_drop=max_drop_length-database_length-48

if [ $max_to_create -gt $max_to_drop ]; then
	allowed_name_length=$max_to_drop
else
	allowed_name_length=$max_to_create
fi

let excess_length=allowed_name_length+1

long_table_name=$(openssl rand -base64 $excess_length | tr -dc A-Za-z | head -c $excess_length)
allowed_table_name=$(openssl rand -base64 $allowed_name_length | tr -dc A-Za-z | head -c $allowed_name_length)

$CLICKHOUSE_CLIENT -mn --query="CREATE TABLE $long_table_name (id UInt32, long_table_name String) Engine=MergeTree() order by id;" 2>&1 | grep -o -m 1 ARGUMENT_OUT_OF_BOUND
$CLICKHOUSE_CLIENT -mn --query="CREATE TABLE $allowed_table_name (id UInt32, allowed_table_name String) Engine=MergeTree() order by id;"
$CLICKHOUSE_CLIENT -mn --query="DROP TABLE $allowed_table_name;"
$CLICKHOUSE_CLIENT -mn --query="DROP TABLE for_get_metadata_path;"
