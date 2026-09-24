#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `obfuscate` has two user-visible entrypoints that interpret its inner query independently:
# `DESCRIBE obfuscate(...)` derives the structure (`TableFunctionObfuscate::getActualTableStructure`),
# and `SELECT ... FROM obfuscate(...)` reads it (`StorageObfuscate::read`). Both have to enforce the
# `SELECT` privilege on the tables of the inner query; table-function regressions over other ClickHouse
# objects have repeatedly shown up on only one of the two paths.

USER="user_05223_${CLICKHOUSE_DATABASE}"
TABLE="${CLICKHOUSE_DATABASE}.src_05223"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS ${TABLE};
CREATE TABLE ${TABLE} (n UInt64, s String) ENGINE = MergeTree ORDER BY n;
INSERT INTO ${TABLE} SELECT number, toString(number) FROM numbers(100);
DROP USER IF EXISTS ${USER};
CREATE USER ${USER} IDENTIFIED WITH no_password;
GRANT CREATE TEMPORARY TABLE ON *.* TO ${USER};
"

CLIENT_AS_USER="${CLICKHOUSE_CLIENT} --user ${USER}"

echo '-- without SELECT on the source table, both entrypoints are denied'
$CLIENT_AS_USER --query "SELECT * FROM obfuscate(SELECT * FROM ${TABLE}) LIMIT 3" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLIENT_AS_USER --query "DESCRIBE obfuscate(SELECT * FROM ${TABLE})" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo '-- a column-level grant admits exactly the granted column on both entrypoints'
$CLICKHOUSE_CLIENT --query "GRANT SELECT(n) ON ${TABLE} TO ${USER}"
$CLIENT_AS_USER --query "SELECT count() FROM (SELECT n FROM obfuscate(SELECT n FROM ${TABLE}) LIMIT 3)"
$CLIENT_AS_USER --query "DESCRIBE obfuscate(SELECT n FROM ${TABLE})" | cut -f1,2
$CLIENT_AS_USER --query "SELECT * FROM obfuscate(SELECT * FROM ${TABLE}) LIMIT 3" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLIENT_AS_USER --query "DESCRIBE obfuscate(SELECT * FROM ${TABLE})" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo '-- with SELECT on the source table, both entrypoints succeed'
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${TABLE} TO ${USER}"
$CLIENT_AS_USER --query "SELECT count() FROM (SELECT * FROM obfuscate(SELECT * FROM ${TABLE}) LIMIT 3)"
$CLIENT_AS_USER --query "DESCRIBE obfuscate(SELECT * FROM ${TABLE})" | cut -f1,2

$CLICKHOUSE_CLIENT --query "
DROP USER IF EXISTS ${USER};
DROP TABLE IF EXISTS ${TABLE};
"
