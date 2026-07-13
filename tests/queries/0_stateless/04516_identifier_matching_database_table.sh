#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Unique per-run names so the test can run in parallel.
DB_ONE="${CLICKHOUSE_DATABASE}_MatchDb"
DB_TWO="${CLICKHOUSE_DATABASE}_MATCHDB"
DB_ONE_FOLDED=$(echo "$DB_ONE" | tr '[:upper:]' '[:lower:]')

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB_ONE}"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB_TWO}"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_ONE}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.MyTable (x Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB_ONE}.MyTable VALUES (7)"

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
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_TWO}.Tab (x Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_TWO}.TAB (y Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM \"${DB_TWO}\".Tab SETTINGS database_and_table_name_matching = 'standard'" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM \"${DB_TWO}\".\"Tab\" SETTINGS database_and_table_name_matching = 'standard'"

echo '--- standard: rename keeps the folded index consistent'
${CLICKHOUSE_CLIENT} --query "RENAME TABLE \"${DB_TWO}\".\"TAB\" TO \"${DB_TWO}\".\"Renamed\" SETTINGS database_and_table_name_matching = 'standard'"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM \"${DB_TWO}\".tab SETTINGS database_and_table_name_matching = 'standard'"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM \"${DB_TWO}\".renamed SETTINGS database_and_table_name_matching = 'standard'"

echo '--- standard: information_schema aliases are not ambiguous'
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM Information_Schema.tables WHERE table_schema = 'system' SETTINGS database_and_table_name_matching = 'standard'"
