#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The tables named in `REPLACE/ATTACH PARTITION ... FROM` and `MOVE PARTITION ... TO TABLE` are bound to the tables
# their hierarchical names denote before the access is checked: inside `USE db`, `ns.src` is the table `ns.src` of
# the database `db`, so a grant on the nonexistent database `ns` must not authorize reading or writing it.
# `BACKUP`, `RESTORE` and `SNAPSHOT` do not resolve hierarchical names, so they accept at most two parts unquoted.

db=$CLICKHOUSE_DATABASE
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function run()
{
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 | sed "s/${CLICKHOUSE_TEST_UNIQUE_NAME}/unique/g; s/${db}/db/g"
}

function run_as()
{
    $CLICKHOUSE_CLIENT --user "${user}" -q "$1" 2>&1 | grep -o -m1 'ACCESS_DENIED'
}

run "CREATE TABLE ${db}.dst (x UInt8) ENGINE = MergeTree ORDER BY x"
run "CREATE TABLE ${db}.\"ns.src\" (x UInt8) ENGINE = MergeTree ORDER BY x"
run "CREATE TABLE ${db}.\"ns.moved\" (x UInt8) ENGINE = MergeTree ORDER BY x"
run "INSERT INTO ${db}.\"ns.src\" VALUES (1)"
run "DROP USER IF EXISTS ${user}"
run "CREATE USER ${user}"
run "GRANT INSERT, ALTER DELETE, ALTER MOVE PARTITION ON ${db}.dst TO ${user}"

echo '--- ATTACH PARTITION FROM ns.src with a grant on the nonexistent database ns only'
run "GRANT SELECT ON ns.* TO ${user}"
run_as "ALTER TABLE dst ATTACH PARTITION tuple() FROM ns.src"
run_as "ALTER TABLE dst REPLACE PARTITION tuple() FROM ns.src"
run "SELECT count() FROM ${db}.dst"

echo '--- and with a grant on the table'
run "GRANT SELECT ON ${db}.\`ns.src\` TO ${user}"
run_as "ALTER TABLE dst ATTACH PARTITION tuple() FROM ns.src"
run "SELECT count() FROM ${db}.dst"
run_as "ALTER TABLE dst REPLACE PARTITION tuple() FROM ${db}.ns.src"
run "SELECT count() FROM ${db}.dst"

echo '--- MOVE PARTITION TO TABLE ns.moved with a grant on the nonexistent database ns only'
run "GRANT INSERT ON ns.* TO ${user}"
run_as "ALTER TABLE dst MOVE PARTITION tuple() TO TABLE ns.moved"
run "SELECT count() FROM ${db}.dst"

echo '--- and with a grant on the table'
run "GRANT INSERT ON ${db}.\`ns.moved\` TO ${user}"
run_as "ALTER TABLE dst MOVE PARTITION tuple() TO TABLE ns.moved"
run "SELECT count() FROM ${db}.dst"
run "SELECT count() FROM ${db}.\`ns.moved\`"

run "DROP USER ${user}"

echo '--- BACKUP, RESTORE and SNAPSHOT accept at most two parts unquoted'
$CLICKHOUSE_FORMAT --oneline --query "BACKUP TABLE a.b.c TO Null" 2>&1 | grep -o -m1 'SYNTAX_ERROR'
$CLICKHOUSE_FORMAT --oneline --query "RESTORE TABLE a.b.c AS a.d FROM Null" 2>&1 | grep -o -m1 'SYNTAX_ERROR'
$CLICKHOUSE_FORMAT --oneline --query "BACKUP TABLE a.\`b.c\` TO Null"
