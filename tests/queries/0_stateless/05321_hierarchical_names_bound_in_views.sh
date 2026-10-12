#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A qualified hierarchical name in a stored query (`ns.t` for the table `ns.t` of the current database) is bound to
# the table it denotes when the view is created, so creating a database `ns` later does not retarget the view.

db=$CLICKHOUSE_DATABASE
ns=${db}_ns

function run()
{
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 | sed -e "s/${ns}/ns/g" -e "s/${db}/db/g"
}

run "CREATE TABLE ${db}.\"${ns}.t\" (x UInt8) ENGINE = Memory"
run "INSERT INTO ${db}.\"${ns}.t\" VALUES (1)"

run "CREATE VIEW ${db}.v AS SELECT x FROM ${ns}.t"
run "CREATE VIEW ${db}.v3 AS SELECT x FROM ${db}.${ns}.t"
run "SHOW CREATE VIEW ${db}.v FORMAT TSVRaw"
run "SHOW CREATE VIEW ${db}.v3 FORMAT TSVRaw"

run "CREATE DATABASE ${ns}"
run "CREATE TABLE ${ns}.t (x UInt8) ENGINE = Memory"
run "INSERT INTO ${ns}.t VALUES (2)"

run "SELECT * FROM ${db}.v"
run "SELECT * FROM ${db}.v3"

run "DROP DATABASE ${ns}"
