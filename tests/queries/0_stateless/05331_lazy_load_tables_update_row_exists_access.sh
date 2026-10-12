#!/usr/bin/env bash

# `UPDATE ... SET _row_exists = 0` is a delete and is governed by `ALTER DELETE`. On a table in a database
# with `lazy_load_tables` that has not been loaded yet, the classification must still see the
# `_row_exists` virtual column of the real storage, not the columns-only metadata of the proxy.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}_lazy"
user_del="${CLICKHOUSE_DATABASE}_del_05331"
user_upd="${CLICKHOUSE_DATABASE}_upd_05331"

$CLICKHOUSE_CLIENT -q "
DROP DATABASE IF EXISTS $db;
DROP USER IF EXISTS $user_del, $user_upd;
CREATE DATABASE $db ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE $db.t (id UInt32, val UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO $db.t SELECT number, number FROM numbers(10);

CREATE USER $user_del IDENTIFIED WITH plaintext_password BY 'password';
GRANT SELECT, ALTER DELETE ON $db.t TO $user_del;
CREATE USER $user_upd IDENTIFIED WITH plaintext_password BY 'password';
GRANT SELECT, ALTER UPDATE ON $db.t TO $user_upd;
"

function reattach()
{
    $CLICKHOUSE_CLIENT -q "DETACH DATABASE $db; ATTACH DATABASE $db;"
    $CLICKHOUSE_CLIENT -q "SELECT 'cold engine', engine FROM system.tables WHERE database = '$db' AND name = 't'"
}

function run_as()
{
    local user=$1
    local id=$2
    $CLICKHOUSE_CLIENT --user "$user" --password password --enable_lightweight_update 1 \
        -q "UPDATE $db.t SET _row_exists = 0 WHERE id = $id" 2>&1 | grep -q -F 'ACCESS_DENIED' && echo "$user denied" || echo "$user allowed"
}

# Only `ALTER UPDATE`: a delete in disguise must be rejected.
reattach
run_as "$user_upd" 1 | sed "s/$user_upd/upd/"
$CLICKHOUSE_CLIENT -q "SELECT 'count', count() FROM $db.t"

# Only `ALTER DELETE`: the same statement is accepted, as on an eagerly loaded table.
reattach
run_as "$user_del" 2 | sed "s/$user_del/del/"
$CLICKHOUSE_CLIENT -q "SELECT 'count', count() FROM $db.t"

$CLICKHOUSE_CLIENT -q "
DROP DATABASE $db;
DROP USER $user_del, $user_upd;
"
