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

echo '--- standard: EXISTS DATABASE and SHOW TABLES FROM throw on an ambiguous fold'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "EXISTS DATABASE ${DB_ONE_FOLDED}" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SHOW TABLES FROM ${DB_ONE_FOLDED}" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq

echo '--- standard: EXISTS DATABASE folds, double-quoted stays exact'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB_TWO}"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "EXISTS DATABASE ${DB_ONE_FOLDED}"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "EXISTS DATABASE \"${DB_ONE_FOLDED}\""

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
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SHOW COLUMNS FROM ${DB_ONE_FOLDED}.newtable"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SHOW COLUMNS FROM \"${DB_ONE_FOLDED}\".newtable"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SHOW INDEXES FROM ${DB_ONE_FOLDED}.newtable"

echo '--- standard: sibling databases make CREATE TABLE, EXISTS and SHOW COLUMNS ambiguous'
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_TWO}"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE ${DB_ONE_FOLDED}.T2 (x Int32) ENGINE = Memory" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "EXISTS TABLE ${DB_ONE_FOLDED}.newtable" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SHOW COLUMNS FROM ${DB_ONE_FOLDED}.newtable" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq

echo '--- standard: EXCHANGE TABLES folds both operands'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB_TWO}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.Foo (x Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.Bar (y Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB_ONE}.Foo VALUES (1)"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "EXCHANGE TABLES ${DB_ONE_FOLDED}.foo AND ${DB_ONE_FOLDED}.bar"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_ONE}.Bar"

echo '--- standard: sibling destination makes EXCHANGE ambiguous'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.\"bar\" (z Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "EXCHANGE TABLES ${DB_ONE_FOLDED}.foo AND ${DB_ONE_FOLDED}.bar" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq

echo '--- standard: RENAME folds the source and keeps the new name as written'
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${DB_ONE}.\"bar\""
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "RENAME TABLE ${DB_ONE_FOLDED}.foo TO ${DB_ONE_FOLDED}.RenamedTo"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB_ONE}.RenamedTo"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB_ONE}.Foo"

echo '--- standard: SYSTEM STOP MERGES folds, double-quoted wrong case stays exact'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SYSTEM STOP MERGES ${DB_ONE_FOLDED}.newtable"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SYSTEM START MERGES ${DB_ONE_FOLDED}.newtable"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "SYSTEM STOP MERGES \"${DB_ONE_FOLDED}\".newtable" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq

echo '--- standard: ALTER DATABASE folds the database name'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "ALTER DATABASE ${DB_ONE_FOLDED} MODIFY COMMENT 'folded alter'"
${CLICKHOUSE_CLIENT} --query "SELECT comment FROM system.databases WHERE name = '${DB_ONE}'"

echo '--- standard: sibling databases make ALTER DATABASE ambiguous'
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_TWO}"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "ALTER DATABASE ${DB_ONE_FOLDED} MODIFY COMMENT 'x'" 2>&1 | grep -oF "AMBIGUOUS_IDENTIFIER" | uniq

echo '--- standard: implicit current database stays exact with a case sibling'
CLIENT_IN_DB_ONE=${CLICKHOUSE_CLIENT/--database=$CLICKHOUSE_DATABASE/--database=$DB_ONE}
${CLIENT_IN_DB_ONE} --database_and_table_name_matching=standard --query "SELECT count() FROM NewTable"
${CLIENT_IN_DB_ONE} --database_and_table_name_matching=standard --query "EXISTS TABLE NewTable"
${CLIENT_IN_DB_ONE} --database_and_table_name_matching=standard --query "SHOW COLUMNS FROM NewTable" | head -1 | cut -f1

echo '--- standard: CREATE TABLE AS folds the source, double-quoted wrong case stays exact'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB_TWO}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.SrcTable (x Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE ${DB_ONE_FOLDED}.CopyTable AS ${DB_ONE_FOLDED}.srctable"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DB_ONE}.CopyTable"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.CopyTwo AS ${DB_ONE_FOLDED}.srctable" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE ${DB_ONE}.CopyTwo AS \"${DB_ONE_FOLDED}\".srctable" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE ${DB_ONE}.CopyTwo AS \"${DB_ONE}\".\"srctable\"" 2>&1 | grep -oF "CANNOT_GET_CREATE_TABLE_QUERY" | uniq

echo '--- standard: CREATE TABLE CLONE AS folds the source name'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.CloneSrc (x Int32) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB_ONE}.CloneSrc VALUES (3)"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE TABLE ${DB_ONE_FOLDED}.CloneDst CLONE AS ${DB_ONE_FOLDED}.clonesrc"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_ONE}.CloneDst"

echo '--- standard: MOVE PARTITION TO TABLE folds, double-quoted wrong case stays exact'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.MoveSrc (x Int32) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.MoveDst (x Int32) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB_ONE}.MoveSrc VALUES (1)"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "ALTER TABLE ${DB_ONE}.MoveSrc MOVE PARTITION tuple() TO TABLE \"${DB_ONE_FOLDED}\".\"movedst\"" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "ALTER TABLE ${DB_ONE}.MoveSrc MOVE PARTITION tuple() TO TABLE \"${DB_ONE}\".\"movedst\"" 2>&1 | grep -oF "UNKNOWN_TABLE" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "ALTER TABLE ${DB_ONE_FOLDED}.movesrc MOVE PARTITION tuple() TO TABLE ${DB_ONE_FOLDED}.movedst"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_ONE}.MoveDst"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DB_ONE}.MoveSrc"

echo '--- standard: REPLACE PARTITION FROM folds, double-quoted wrong case stays exact'
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "ALTER TABLE ${DB_ONE_FOLDED}.movesrc REPLACE PARTITION tuple() FROM ${DB_ONE_FOLDED}.movedst"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_ONE}.MoveSrc"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "ALTER TABLE ${DB_ONE}.MoveSrc REPLACE PARTITION tuple() FROM \"${DB_ONE_FOLDED}\".\"movedst\"" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq

echo '--- standard: materialized view TO target folds and is stored canonically'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_ONE}.MvTarget (x Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE MATERIALIZED VIEW ${DB_ONE_FOLDED}.MvView TO ${DB_ONE_FOLDED}.mvtarget AS SELECT x FROM ${DB_ONE}.SrcTable"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${DB_ONE}.MvView" | grep -c "TO ${DB_ONE}\.MvTarget"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB_ONE}.SrcTable VALUES (5)"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_ONE}.MvTarget"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW ${DB_ONE}.MvBad TO ${DB_ONE_FOLDED}.mvtarget AS SELECT x FROM ${DB_ONE}.SrcTable" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq
${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --query "CREATE MATERIALIZED VIEW ${DB_ONE}.MvBad TO \"${DB_ONE_FOLDED}\".mvtarget AS SELECT x FROM ${DB_ONE}.SrcTable" 2>&1 | grep -oF "UNKNOWN_DATABASE" | uniq
