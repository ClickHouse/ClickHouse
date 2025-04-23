#!/usr/bin/expect -f
set CLICKHOUSE_CLIENT "../../../build/programs/clickhouse client"

set timeout 30
log_user 1

spawn bash
send "set -e\r"

send "export CLICKHOUSE_CLIENT\r"

send "$CLICKHOUSE_CLIENT --query 'CREATE DATABASE IF NOT EXISTS test;' >/dev/null 2>&1\r"
expect "\r"

send "$CLICKHOUSE_CLIENT --query 'CREATE TABLE IF NOT EXISTS test.simple_table (id UInt64, name String) ENGINE = Memory;' >/dev/null 2>&1\r"
expect "\r"

send "$CLICKHOUSE_CLIENT --processed-rows --query \"INSERT INTO test.simple_table SELECT 1 AS id, 'Alice' AS name;\" \r"
expect "Processed rows: 1"

send "$CLICKHOUSE_CLIENT --processed-rows --query \"INSERT INTO test.simple_table SELECT number + 1 AS id, \['Alice','Bob','Charlie'\]\[number + 1\] AS name FROM numbers(3);\" \r"
expect "Processed rows: 3"

send "echo OK\r"
expect "OK"

send "exit\r"
expect eof
