#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Durable references created under standard matching must be stored canonically, so they keep
# working from sensitive-mode sessions and after a metadata reload.
DB="${CLICKHOUSE_DATABASE}_PersistDb"
DB_FOLDED=$(echo "$DB" | tr '[:upper:]' '[:lower:]')
RO_USER="u_04519_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${RO_USER}"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}"
}
trap cleanup EXIT
cleanup

# Column matching is implemented only for the analyzer; force it so old-analyzer suites pass.
STANDARD="${CLICKHOUSE_CLIENT} --enable_analyzer=1 --database_and_table_name_matching=standard --column_and_query_name_matching=standard"

# Atomic pinned explicitly: the test detaches and re-attaches this database to force a metadata reload.
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB} ENGINE = Atomic"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB}.SrcTable (Val Int32) ENGINE = MergeTree ORDER BY Val"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB}.MvTarget (Val Int32) ENGINE = MergeTree ORDER BY Val"

echo '--- standard: MV with a folded TO target stores the canonical spelling'
${STANDARD} --query "CREATE MATERIALIZED VIEW ${DB_FOLDED}.PersistMv TO ${DB_FOLDED}.mvtarget AS SELECT \"Val\" FROM \"${DB}\".\"SrcTable\""
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${DB}.PersistMv" | grep -c "TO ${DB}\.MvTarget"

echo '--- standard: view created through folded outer references stores the canonical database'
${STANDARD} --query "CREATE VIEW ${DB_FOLDED}.PersistView AS SELECT \"Val\" FROM \"${DB}\".\"SrcTable\""
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${DB}.PersistView" | grep -c "CREATE VIEW ${DB}\.PersistView"

echo '--- standard: folded FROM spellings in the stored body are canonicalized'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB}.FoldedTarget (Val Int32) ENGINE = MergeTree ORDER BY Val"
${STANDARD} --query "CREATE MATERIALIZED VIEW ${DB_FOLDED}.FoldedMv TO ${DB_FOLDED}.foldedtarget AS SELECT Val FROM ${DB_FOLDED}.srctable"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${DB}.FoldedMv" | grep -c "FROM ${DB}\.SrcTable"
${STANDARD} --query "CREATE VIEW ${DB_FOLDED}.FoldedView AS SELECT Val FROM ${DB_FOLDED}.srctable"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${DB}.FoldedView" | grep -c "FROM ${DB}\.SrcTable"

echo '--- standard: folded column spellings in the stored body are rejected at CREATE'
${STANDARD} --query "CREATE MATERIALIZED VIEW ${DB_FOLDED}.BadMv TO ${DB_FOLDED}.foldedtarget AS SELECT val FROM \"${DB}\".\"SrcTable\"" 2>&1 | grep -oF "INCORRECT_QUERY" | uniq
${STANDARD} --query "CREATE VIEW ${DB_FOLDED}.BadView AS SELECT val FROM ${DB_FOLDED}.srctable" 2>&1 | grep -oF "INCORRECT_QUERY" | uniq

echo '--- sensitive session: INSERT pushes through the MV, stored references ignore session mode'
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB}.SrcTable VALUES (1)"
${CLICKHOUSE_CLIENT} --query "SELECT Val FROM ${DB}.MvTarget ORDER BY Val"
${CLICKHOUSE_CLIENT} --query "SELECT Val FROM ${DB}.FoldedTarget ORDER BY Val"
${CLICKHOUSE_CLIENT} --query "SELECT Val FROM ${DB}.FoldedView ORDER BY Val"

echo '--- metadata reload: DETACH + ATTACH, sensitive session still uses the view and the MV'
${CLICKHOUSE_CLIENT} --query "DETACH DATABASE ${DB}"
${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE ${DB}"
${CLICKHOUSE_CLIENT} --query "SELECT Val FROM ${DB}.PersistView ORDER BY Val"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB}.SrcTable VALUES (2)"
${CLICKHOUSE_CLIENT} --query "SELECT Val FROM ${DB}.MvTarget ORDER BY Val"
${CLICKHOUSE_CLIENT} --query "SELECT Val FROM ${DB}.PersistMv ORDER BY Val"
${CLICKHOUSE_CLIENT} --query "SELECT Val FROM ${DB}.FoldedTarget ORDER BY Val"
${CLICKHOUSE_CLIENT} --query "SELECT Val FROM ${DB}.FoldedView ORDER BY Val"

echo '--- a sensitive-mode user reads the view created by the standard-mode session'
${CLICKHOUSE_CLIENT} --query "CREATE USER ${RO_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${DB}.* TO ${RO_USER}"
${CLICKHOUSE_CLIENT} --user ${RO_USER} --query "SELECT Val FROM ${DB}.PersistView ORDER BY Val"
