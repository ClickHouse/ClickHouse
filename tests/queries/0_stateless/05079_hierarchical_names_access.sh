#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The access to a table written with a hierarchical name (`db.ns.t` for the table `ns.t` of the database `db`) is checked
# for the database and the table the name resolves to, not for the name as written: a grant on the nonexistent
# database `db.ns` must not allow creating, renaming or dropping tables of the database `db`.

db=$CLICKHOUSE_DATABASE
creator="creator_${CLICKHOUSE_TEST_UNIQUE_NAME}"
renamer="renamer_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function run()
{
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 | sed "s/${db}/db/g; s/${creator}/creator/g; s/${renamer}/renamer/g"
}

function run_as()
{
    $CLICKHOUSE_CLIENT --user "$1" -q "$2" 2>&1 | sed "s/${db}/db/g; s/${creator}/creator/g; s/${renamer}/renamer/g"
}

run "CREATE TABLE ${db}.\"ns.existing\" (x UInt8) ENGINE = Memory"
run "DROP USER IF EXISTS ${creator}, ${renamer}"
run "CREATE USER ${creator}"
run "CREATE USER ${renamer}"
# The test configuration sets `table_engines_require_grant`: the engine is granted so that the only thing that can deny a `CREATE TABLE` is the database.
run "GRANT TABLE ENGINE ON Memory TO ${creator}"

echo '--- CREATE TABLE db.ns.c with a grant on the nonexistent database db.ns only'
run "GRANT CREATE TABLE ON \"${db}.ns\".* TO ${creator}"
run_as "${creator}" "CREATE TABLE ${db}.ns.c (x UInt8) ENGINE = Memory" | grep -o 'ACCESS_DENIED' | sort -u
run "EXISTS TABLE ${db}.\"ns.c\""
run "GRANT SHOW DATABASES ON ${db}.* TO ${creator}"
$CLICKHOUSE_CLIENT --user "${creator}" -m -q "USE ${db}.ns; CREATE TABLE c (x UInt8) ENGINE = Memory" 2>&1 | grep -o 'ACCESS_DENIED' | sort -u
run "EXISTS TABLE ${db}.\"ns.c\""

echo '--- and with a grant on the database db'
run "GRANT CREATE TABLE ON ${db}.* TO ${creator}"
run_as "${creator}" "CREATE TABLE ${db}.ns.c (x UInt8) ENGINE = Memory"
run "EXISTS TABLE ${db}.\"ns.c\""
run "SHOW TABLES FROM ${db}"

echo '--- RENAME TABLE db.ns.c with grants on the nonexistent database db.ns only'
run "GRANT SELECT, DROP TABLE ON \"${db}.ns\".* TO ${renamer}"
run "GRANT CREATE TABLE, INSERT ON ${db}.* TO ${renamer}"
run_as "${renamer}" "RENAME TABLE ${db}.ns.c TO ${db}.taken" | grep -o 'ACCESS_DENIED' | sort -u
run "EXISTS TABLE ${db}.\"ns.c\""
run "EXISTS TABLE ${db}.taken"

echo '--- and with grants on the database db'
run "GRANT SELECT, DROP TABLE ON ${db}.* TO ${renamer}"
run_as "${renamer}" "RENAME TABLE ${db}.ns.c TO ${db}.taken"
run "EXISTS TABLE ${db}.\"ns.c\""
run "EXISTS TABLE ${db}.taken"

echo '--- the destination of RENAME is checked as the table it is placed as'
run "REVOKE CREATE TABLE, INSERT ON ${db}.* FROM ${renamer}"
run "GRANT CREATE TABLE, INSERT ON \"${db}.ns\".* TO ${renamer}"
run_as "${renamer}" "RENAME TABLE ${db}.taken TO ${db}.ns.c" | grep -o 'ACCESS_DENIED' | sort -u
run "EXISTS TABLE ${db}.\"ns.c\""
run "EXISTS TABLE ${db}.taken"

echo '--- DROP TABLE db.ns.existing with a grant on the nonexistent database db.ns only'
run "GRANT DROP TABLE ON \"${db}.ns\".* TO ${creator}"
run_as "${creator}" "DROP TABLE ${db}.ns.existing" | grep -o 'ACCESS_DENIED' | sort -u
run "EXISTS TABLE ${db}.\"ns.existing\""

run "DROP USER ${creator}, ${renamer}"
