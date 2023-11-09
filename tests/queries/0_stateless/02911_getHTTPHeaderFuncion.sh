#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "SELECT getClientHTTPHeader('key')" | curl -s -H 'X-ClickHouse-User: default' -H 'X-ClickHouse-Key: ' -H 'key: value' 'http://localhost:8123/' -d @-  

echo "SELECT getClientHTTPHeader('key1'), getClientHTTPHeader('key2')" | curl -s -H 'X-Clickhouse-User: default' \
    -H 'X-ClickHouse-Key: ' -H 'key1: value1' -H 'key2: value2' 'http://localhost:8123/' -d @-

echo "SELECT getClientHTTPHeader('test-' || 'key' || '-1'), getClientHTTPHeader('test-key-1'), getClientHTTPHeader('key2')" | curl -s -H 'X-Clickhouse-User: default' \
    -H 'X-ClickHouse-Key: ' -H 'test-key-1: value1' -H 'key2: value2' 'http://localhost:8123/' -d @-

#Code: 36. DB::Exception: NOT-FOUND-KEY is not in HTTP request headers
echo "SELECT getClientHTTPHeader('NOT-FOUND-KEY')"| curl -s -H 'X-Clickhouse-User: default' \
    -H 'X-ClickHouse-Key: ' -H 'key1: value1' -H 'key2: value2' 'http://localhost:8123/' -d @- | grep -o -e BAD_ARGUMENTS

#Code: 36. DB::Exception: The header FORBIDDEN-KEY is in headers_forbidden_to_return_list, you can config it in config file.
echo "SELECT getClientHTTPHeader('FORBIDDEN-KEY')" | curl -s -H 'X-ClickHouse-User: default' -H 'X-ClickHouse-Key: ' -H 'FORBIDDEN-KEY1: forbbiden1' 'http://localhost:8123/' -d @-  | grep -o -e BAD_ARGUMENTS
echo "SELECT getClientHTTPHeader('FORBIDDEN-KEY')" | curl -s -H 'X-ClickHouse-User: default' -H 'X-ClickHouse-Key: ' -H 'FORBIDDEN-KEY2: forbbiden2' 'http://localhost:8123/' -d @-  | grep -o -e BAD_ARGUMENTS

db_name=${CLICKHOUSE_DATABASE}

$CLICKHOUSE_CLIENT -q "CREATE DATABASE IF NOT EXISTS ${db_name};"

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db_name}.02884_get_http_header
     (id UInt32, 
     http_key1 String DEFAULT getClientHTTPHeader('http_header_key1'),
     http_key2 String DEFAULT getClientHTTPHeader('http_header_key2'),
     http_key3 String DEFAULT getClientHTTPHeader('http_header_key3'),
     http_key4 String DEFAULT getClientHTTPHeader('http_header_key4'),
     http_key5 String DEFAULT getClientHTTPHeader('http_header_key5'),
     http_key6 String DEFAULT getClientHTTPHeader('http_header_key6'),
     http_key7 String DEFAULT getClientHTTPHeader('http_header_key7')
     ) 
     Engine=MergeTree()
     ORDER BY id" 

#Insert data via http request
echo "INSERT INTO ${db_name}.02884_get_http_header (id) values (1)" | curl -s -H 'X-ClickHouse-User: default' -H 'X-ClickHouse-Key: ' \
 -H 'http_header_key1: row1_value1'\
 -H 'http_header_key2: row1_value2'\
 -H 'http_header_key3: row1_value3'\
 -H 'http_header_key4: row1_value4'\
 -H 'http_header_key5: row1_value5'\
 -H 'http_header_key6: row1_value6'\
 -H 'http_header_key7: row1_value7' 'http://localhost:8123/' -d @-

echo "INSERT INTO ${db_name}.02884_get_http_header (id) values (2)" | curl -s -H 'X-ClickHouse-User: default' -H 'X-ClickHouse-Key: ' \
 -H 'http_header_key1: row2_value1'\
 -H 'http_header_key2: row2_value2'\
 -H 'http_header_key3: row2_value3'\
 -H 'http_header_key4: row2_value4'\
 -H 'http_header_key5: row2_value5'\
 -H 'http_header_key6: row2_value6'\
 -H 'http_header_key7: row2_value7' 'http://localhost:8123/' -d @-

$CLICKHOUSE_CLIENT -q "SELECT id, http_key1, http_key2, http_key3, http_key4, http_key5, http_key6, http_key7 FROM ${db_name}.02884_get_http_header ORDER BY id;"
#Insert data via tcp client
$CLICKHOUSE_CLIENT --param_db="$db_name" -q "INSERT INTO ${db_name}.02884_get_http_header (id) values (3)"
$CLICKHOUSE_CLIENT --param_db="$db_name" -q "SELECT * FROM ${db_name}.02884_get_http_header where id = 3"

echo "SELECT getClientHTTPHeader('key_from_query_1'), getClientHTTPHeader('key_from_query_2'), getClientHTTPHeader('key_from_query_3'), * FROM ${db_name}.02884_get_http_header ORDER BY id" | curl -s -H 'X-Clickhouse-User: default' \
    -H 'X-ClickHouse-Key: ' -H 'key_from_query_1: value_from_query_1' -H 'key_from_query_2: value_from_query_2' -H 'key_from_query_3: value_from_query_3' 'http://localhost:8123/' -d @-

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${db_name}.02884_get_http_header"

$CLICKHOUSE_CLIENT -q "CREATE TABLE IF NOT EXISTS ${db_name}.02884_header_from_table (header_name String) Engine=Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db_name}.02884_header_from_table values ('http_key1'), ('http_key2')"

echo "SELECT getClientHTTPHeader(header_name) as value from  (select * FROM ${db_name}.02884_header_from_table) order by value" | curl -s -H 'X-Clickhouse-User: default' \
    -H 'X-ClickHouse-Key: ' -H 'http_key1: http_value1' -H 'http_key2: http_value2' 'http://localhost:8123/' -d @-

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${db_name}"
