#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: creates case-sibling databases, impossible on case-insensitive filesystems (macOS).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Unique per-run names so the test can run in parallel.
DB_ONE="${CLICKHOUSE_DATABASE}_MatchDb"
DB_TWO="${CLICKHOUSE_DATABASE}_MATCHDB"
DB_ONE_FOLDED=$(echo "$DB_ONE" | tr '[:upper:]' '[:lower:]')

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB_ONE}; DROP DATABASE IF EXISTS ${DB_TWO}"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_ONE}; CREATE TABLE ${DB_ONE}.MyTable (x Int32) ENGINE = Memory; INSERT INTO ${DB_ONE}.MyTable VALUES (7)"

echo '--- sensitive default: exact works, folded fails'
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_ONE}.MyTable"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_ONE_FOLDED}.mytable" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq

echo '--- standard: folded database and table resolve'
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_ONE_FOLDED}.mytable SETTINGS database_and_table_name_matching = 'standard'"

echo '--- standard: double-quoted parts stay exact'
${CLICKHOUSE_CLIENT} --query "SELECT x FROM \"${DB_ONE}\".\"MyTable\" SETTINGS database_and_table_name_matching = 'standard'"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM \"${DB_ONE_FOLDED}\".MyTable SETTINGS database_and_table_name_matching = 'standard'" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq

echo '--- standard: sibling databases are ambiguous even for the exact spelling'
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_TWO}"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_ONE}.MyTable SETTINGS database_and_table_name_matching = 'standard'" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq
${CLICKHOUSE_CLIENT} --query "SELECT x FROM \"${DB_ONE}\".MyTable SETTINGS database_and_table_name_matching = 'standard'"

echo '--- standard: sibling tables are ambiguous even for the exact spelling'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_TWO}.Tab (x Int32) ENGINE = Memory; CREATE TABLE ${DB_TWO}.TAB (y Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM \"${DB_TWO}\".Tab SETTINGS database_and_table_name_matching = 'standard'" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM \"${DB_TWO}\".\"Tab\" SETTINGS database_and_table_name_matching = 'standard'"

echo '--- standard: rename keeps the folded index consistent'
${CLICKHOUSE_CLIENT} --query "RENAME TABLE \"${DB_TWO}\".\"TAB\" TO \"${DB_TWO}\".\"Renamed\" SETTINGS database_and_table_name_matching = 'standard'; SELECT count() FROM \"${DB_TWO}\".tab SETTINGS database_and_table_name_matching = 'standard'; SELECT count() FROM \"${DB_TWO}\".renamed SETTINGS database_and_table_name_matching = 'standard'"

echo '--- standard: information_schema aliases are not ambiguous'
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM Information_Schema.tables WHERE table_schema = 'system' SETTINGS database_and_table_name_matching = 'standard'"

echo '--- standard: EXISTS DATABASE and SHOW TABLES FROM throw on an ambiguous fold'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "EXISTS DATABASE ${DB_ONE_FOLDED}" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SHOW TABLES FROM ${DB_ONE_FOLDED}" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq

echo '--- standard: EXISTS DATABASE folds, double-quoted stays exact'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB_TWO}"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "EXISTS DATABASE ${DB_ONE_FOLDED}; EXISTS DATABASE \"${DB_ONE_FOLDED}\""

echo '--- standard: USE folds the database name'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "USE ${DB_ONE_FOLDED}; SELECT currentDatabase() = '${DB_ONE}'"

echo '--- standard: double-quoted wrong-case SHOW CREATE TABLE stays exact'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SHOW CREATE TABLE \"${DB_ONE_FOLDED}\".mytable" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SHOW CREATE TABLE \"${DB_ONE}\".\"mytable\"" 2>&1 | grep -oF "CANNOT_GET_CREATE_TABLE_QUERY" | uniq

echo '--- standard: double-quoted wrong-case DROP TABLE stays exact and leaves the table intact'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "DROP TABLE \"${DB_ONE}\".\"mytable\"" 2>&1 | grep -oF "UNKNOWN_TABLE" | uniq
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DB_ONE}.MyTable"

echo '--- standard: folded DROP TABLE drops the exact-cased table'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "DROP TABLE ${DB_ONE_FOLDED}.mytable"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB_ONE}.MyTable"

echo '--- standard: CREATE TABLE folds the database component, table name stays as written'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE ${DB_ONE_FOLDED}.NewTable (x Int32) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DB_ONE}.NewTable"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE_FOLDED}.Other (x Int32) ENGINE = Memory" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE \"${DB_ONE_FOLDED}\".Other (x Int32) ENGINE = Memory" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq

echo '--- standard: EXISTS TABLE folds, double-quoted stays exact'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "EXISTS TABLE ${DB_ONE_FOLDED}.newtable"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB_ONE_FOLDED}.newtable"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "EXISTS TABLE \"${DB_ONE_FOLDED}\".newtable"

echo '--- standard: SHOW COLUMNS and SHOW INDEXES fold, double-quoted database matches nothing'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SHOW COLUMNS FROM ${DB_ONE_FOLDED}.newtable; SHOW COLUMNS FROM \"${DB_ONE_FOLDED}\".newtable; SHOW INDEXES FROM ${DB_ONE_FOLDED}.newtable"

echo '--- standard: sibling databases make CREATE TABLE, EXISTS and SHOW COLUMNS ambiguous'
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_TWO}"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE ${DB_ONE_FOLDED}.T2 (x Int32) ENGINE = Memory" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "EXISTS TABLE ${DB_ONE_FOLDED}.newtable" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SHOW COLUMNS FROM ${DB_ONE_FOLDED}.newtable" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq

echo '--- standard: EXCHANGE TABLES folds both operands'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB_TWO}; CREATE TABLE ${DB_ONE}.Foo (x Int32) ENGINE = Memory; CREATE TABLE ${DB_ONE}.Bar (y Int32) ENGINE = Memory; INSERT INTO ${DB_ONE}.Foo VALUES (1)"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "EXCHANGE TABLES ${DB_ONE_FOLDED}.foo AND ${DB_ONE_FOLDED}.bar"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_ONE}.Bar"

echo '--- standard: sibling destination makes EXCHANGE ambiguous'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.\"bar\" (z Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "EXCHANGE TABLES ${DB_ONE_FOLDED}.foo AND ${DB_ONE_FOLDED}.bar" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq

echo '--- standard: RENAME folds the source and keeps the new name as written'
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${DB_ONE}.\"bar\""
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "RENAME TABLE ${DB_ONE_FOLDED}.foo TO ${DB_ONE_FOLDED}.RenamedTo"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB_ONE}.RenamedTo; EXISTS TABLE ${DB_ONE}.Foo"

echo '--- standard: SYSTEM STOP MERGES folds, double-quoted wrong case stays exact'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SYSTEM STOP MERGES ${DB_ONE_FOLDED}.newtable; SYSTEM START MERGES ${DB_ONE_FOLDED}.newtable"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SYSTEM STOP MERGES \"${DB_ONE_FOLDED}\".newtable" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq
