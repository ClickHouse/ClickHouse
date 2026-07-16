#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Continuation of 04516_identifier_matching_database_table: DDL statements under standard matching.
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

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_ONE}; CREATE TABLE ${DB_ONE}.NewTable (x Int32) ENGINE = MergeTree ORDER BY x"

echo '--- standard: ALTER DATABASE folds the database name'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "ALTER DATABASE ${DB_ONE_FOLDED} MODIFY COMMENT 'folded alter'"
${CLICKHOUSE_CLIENT} --query "SELECT comment FROM system.databases WHERE name = '${DB_ONE}'"

echo '--- standard: sibling databases make ALTER DATABASE ambiguous'
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_TWO}"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "ALTER DATABASE ${DB_ONE_FOLDED} MODIFY COMMENT 'x'" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq

echo '--- standard: implicit current database stays exact with a case sibling'
CLIENT_IN_DB_ONE=${CLICKHOUSE_CLIENT/--database=$CLICKHOUSE_DATABASE/--database=$DB_ONE}
${CLIENT_IN_DB_ONE} --database_and_table_name_matching=standard --query "SELECT count() FROM NewTable; EXISTS TABLE NewTable"
${CLIENT_IN_DB_ONE} --database_and_table_name_matching=standard --query "SHOW COLUMNS FROM NewTable" | head -1 | cut -f1

echo '--- standard: CREATE TABLE AS folds the source, double-quoted wrong case stays exact'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB_TWO}; CREATE TABLE ${DB_ONE}.SrcTable (x Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE ${DB_ONE_FOLDED}.CopyTable AS ${DB_ONE_FOLDED}.srctable"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DB_ONE}.CopyTable"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.CopyTwo AS ${DB_ONE_FOLDED}.srctable" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE ${DB_ONE}.CopyTwo AS \"${DB_ONE_FOLDED}\".srctable" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE ${DB_ONE}.CopyTwo AS \"${DB_ONE}\".\"srctable\"" 2>&1 | grep -oF "CANNOT_GET_CREATE_TABLE_QUERY" | uniq

echo '--- standard: CREATE TABLE CLONE AS folds the source name'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.CloneSrc (x Int32) ENGINE = MergeTree ORDER BY x; INSERT INTO ${DB_ONE}.CloneSrc VALUES (3)"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE ${DB_ONE_FOLDED}.CloneDst CLONE AS ${DB_ONE_FOLDED}.clonesrc"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_ONE}.CloneDst"

echo '--- standard: MOVE PARTITION TO TABLE folds, double-quoted wrong case stays exact'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.MoveSrc (x Int32) ENGINE = MergeTree ORDER BY x; CREATE TABLE ${DB_ONE}.MoveDst (x Int32) ENGINE = MergeTree ORDER BY x; INSERT INTO ${DB_ONE}.MoveSrc VALUES (1)"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "ALTER TABLE ${DB_ONE}.MoveSrc MOVE PARTITION tuple() TO TABLE \"${DB_ONE_FOLDED}\".\"movedst\"" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "ALTER TABLE ${DB_ONE}.MoveSrc MOVE PARTITION tuple() TO TABLE \"${DB_ONE}\".\"movedst\"" 2>&1 | grep -oF "UNKNOWN_TABLE" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "ALTER TABLE ${DB_ONE_FOLDED}.movesrc MOVE PARTITION tuple() TO TABLE ${DB_ONE_FOLDED}.movedst"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_ONE}.MoveDst; SELECT count() FROM ${DB_ONE}.MoveSrc"

echo '--- standard: REPLACE PARTITION FROM folds, double-quoted wrong case stays exact'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "ALTER TABLE ${DB_ONE_FOLDED}.movesrc REPLACE PARTITION tuple() FROM ${DB_ONE_FOLDED}.movedst"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_ONE}.MoveSrc"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "ALTER TABLE ${DB_ONE}.MoveSrc REPLACE PARTITION tuple() FROM \"${DB_ONE_FOLDED}\".\"movedst\"" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq

echo '--- standard: materialized view TO target folds and is stored canonically'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.MvTarget (x Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE MATERIALIZED VIEW ${DB_ONE_FOLDED}.MvView TO ${DB_ONE_FOLDED}.mvtarget AS SELECT x FROM ${DB_ONE}.SrcTable"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${DB_ONE}.MvView" | grep -c "TO ${DB_ONE}\.MvTarget"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB_ONE}.SrcTable VALUES (5); SELECT x FROM ${DB_ONE}.MvTarget"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW ${DB_ONE}.MvBad TO ${DB_ONE_FOLDED}.mvtarget AS SELECT x FROM ${DB_ONE}.SrcTable" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE MATERIALIZED VIEW ${DB_ONE}.MvBad TO \"${DB_ONE_FOLDED}\".mvtarget AS SELECT x FROM ${DB_ONE}.SrcTable" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq

echo '--- standard: CREATE INDEX folds, double-quoted wrong case stays exact'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE INDEX Idx1 ON \"${DB_ONE}\".\"newtable\" (x) TYPE minmax" 2>&1 | grep -oF "UNKNOWN_TABLE" | uniq
${CLICKHOUSE_CLIENT} --query "CREATE INDEX Idx1 ON ${DB_ONE_FOLDED}.newtable (x) TYPE minmax" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE INDEX Idx1 ON ${DB_ONE_FOLDED}.newtable (x) TYPE minmax"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.data_skipping_indices WHERE database = '${DB_ONE}' AND table = 'NewTable' AND name = 'Idx1'"

echo '--- standard: DROP INDEX folds, double-quoted wrong case stays exact'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "DROP INDEX Idx1 ON \"${DB_ONE}\".\"newtable\"" 2>&1 | grep -oF "UNKNOWN_TABLE" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "DROP INDEX Idx1 ON ${DB_ONE_FOLDED}.newtable"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.data_skipping_indices WHERE database = '${DB_ONE}' AND table = 'NewTable'"

echo '--- legacy analyzer: folded SELECT binds the canonical qualifier'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.LegacyTable (x Int32) ENGINE = Memory; INSERT INTO ${DB_ONE}.LegacyTable VALUES (7)"
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=0 --database_and_table_name_matching=standard --query "SELECT LegacyTable.x FROM ${DB_ONE_FOLDED}.legacytable"
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=0 --database_and_table_name_matching=standard --query "SELECT x FROM \"${DB_ONE}\".\"legacytable\"" 2>&1 | grep -oF "UNKNOWN_TABLE" | uniq
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=0 --query "SELECT LegacyTable.x FROM ${DB_ONE}.LegacyTable"

echo '--- legacy analyzer: cross join with folded names resolves consistently'
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=0 --database_and_table_name_matching=standard --query "SELECT count() FROM ${DB_ONE_FOLDED}.legacytable, ${DB_ONE_FOLDED}.srctable"

echo '--- legacy analyzer: sibling databases stay ambiguous'
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_TWO}"
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=0 --database_and_table_name_matching=standard --query "SELECT x FROM ${DB_ONE_FOLDED}.legacytable" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq

echo '--- standard: unquoted case-sibling creation is rejected, quoted is allowed'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB_TWO}"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE ${DB_ONE}.CaseTab (x Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE ${DB_ONE}.CASETAB (y Int32) ENGINE = Memory" 2>&1 | grep -oF "differs only in character case" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE ${DB_ONE}.\"CASETAB\" (y Int32) ENGINE = Memory; SELECT count() FROM ${DB_ONE}.\"CASETAB\""
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE DATABASE ${DB_ONE_FOLDED}" 2>&1 | grep -oF "differs only in character case" | uniq
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.SENS1 (x Int32) ENGINE = Memory; CREATE TABLE ${DB_ONE}.sens1 (x Int32) ENGINE = Memory; SELECT count() FROM system.tables WHERE database = '${DB_ONE}' AND lower(name) = 'sens1'"
