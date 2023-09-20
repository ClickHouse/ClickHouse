#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
db="02884_getHttpHeaderFunction"

$CLICKHOUSE_CLIENT -q "CREATE DATABASE IF NOT EXISTS ${db}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE IF NOT EXISTS ${db}.get_http_header (id UInt32, header_value String DEFAULT getHttpHeader('X-Clickhouse-User')) Engine=Memory()" 

#Insert data via tcp client
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.get_http_header (id) values (1), (2)" 

#Insert data via http request
echo "INSERT INTO ${db}.get_http_header (id) values (3), (4)" | curl -H 'X-ClickHouse-User: default' -H 'X-ClickHouse-Key: ' 'http://localhost:8123/' -d @-  

$CLICKHOUSE_CLIENT -q "SELECT * FROM ${db}.get_http_header ORDER BY id;" 
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${db}" 

echo "SELECT getHttpHeader('X-Clickhouse-User')" | curl -H 'X-ClickHouse-User: default' -H 'X-ClickHouse-Key: ' 'http://localhost:8123/' -d @-  

