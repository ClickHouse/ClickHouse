#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db1="$CLICKHOUSE_DATABASE"_02961_db1
db2="$CLICKHOUSE_DATABASE"_02961_db2

$CLICKHOUSE_CLIENT "
DROP DATABASE IF EXISTS $db1;
CREATE DATABASE IF NOT EXISTS $db1;
DROP DATABASE IF EXISTS $db2;
CREATE DATABASE IF NOT EXISTS $db2;


CREATE TABLE IF NOT EXISTS $db1.02961_tb1 (id UInt32) Engine=Memory();
CREATE TABLE IF NOT EXISTS $db1.02961_tb2 (id UInt32) Engine=Memory();

CREATE TABLE IF NOT EXISTS $db2.02961_tb3 (id UInt32) Engine=Memory();
CREATE TABLE IF NOT EXISTS $db2.02961_tb4 (id UInt32) Engine=Memory();
CREATE TABLE IF NOT EXISTS $db2.02961_tb5 (id UInt32) Engine=Memory();

DROP TABLE $db1.02961_tb1, $db1.02961_tb2, $db2.02961_tb3;

SELECT '-- check which tables exist in 02961_db1';
SHOW TABLES FROM $db1;
SELECT '-- check which tables exist in 02961_db2';
SHOW TABLES FROM $db2;"

$CLICKHOUSE_CLIENT "SELECT 'Test when deletion of existing table fails'"

$CLICKHOUSE_CLIENT "DROP TABLE $db2.02961_tb4, $db1.02961_tb1, $db2.02961_tb5" 2>&1 | grep -q "UNKNOWN_TABLE" || echo "Missing UNKNOWN_TABLE error"

$CLICKHOUSE_CLIENT "
SELECT '-- check which tables exist in 02961_db1';
SHOW TABLES FROM $db1;
SELECT '-- check which tables exist in 02961_db2';
SHOW TABLES FROM $db2;

DROP TABLE IF EXISTS tab1, tab2, tab3;
CREATE TABLE IF NOT EXISTS tab1 (id UInt32) Engine=Memory();
CREATE TABLE IF NOT EXISTS tab2 (id UInt32) Engine=Memory();
CREATE TABLE IF NOT EXISTS tab3 (id UInt32) Engine=Memory();

INSERT INTO tab2 SELECT number FROM system.numbers limit 10;
"

$CLICKHOUSE_CLIENT "DROP TABLE IF EMPTY tab1, tab2, tab3" 2>&1 | grep -q "TABLE_NOT_EMPTY" || echo "Missing TABLE_NOT_EMPTY error"

$CLICKHOUSE_CLIENT "
SELECT 'Test when deletion of not empty table fails';
SHOW TABLES;
"

$CLICKHOUSE_CLIENT "TRUNCATE TABLE tab2, tab3" 2>&1 | grep -q "SYNTAX_ERROR" || echo "Missing SYNTAX_ERROR error"

$CLICKHOUSE_CLIENT "
DROP TABLE IF EXISTS tab1, tab2, tab3;

DROP DATABASE IF EXISTS $db1;
DROP DATABASE IF EXISTS $db2;
"
