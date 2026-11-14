#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `UPDATE ... SET _row_exists = 0` is a delete and requires `ALTER DELETE`, not `ALTER UPDATE`. A table of
# a database with `lazy_load_tables` is kept in the catalog as a stand-in that does not know the virtual
# columns of the engine, so after a reload the assignment used to be classified as a regular update.

DB="${CLICKHOUSE_DATABASE}_lazy"
user_del="${CLICKHOUSE_DATABASE}_del_05333"
user_upd="${CLICKHOUSE_DATABASE}_upd_05333"

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DB SYNC;
DROP USER IF EXISTS $user_del, $user_upd;
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE $DB.t (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO $DB.t SELECT number, number FROM numbers(10);

CREATE USER $user_del IDENTIFIED WITH plaintext_password BY 'password';
GRANT SELECT, ALTER DELETE ON $DB.t TO $user_del;
CREATE USER $user_upd IDENTIFIED WITH plaintext_password BY 'password';
GRANT SELECT, ALTER UPDATE ON $DB.t TO $user_upd;
"

# Reloading the database installs the stand-in, as a server restart does.
reattach() { $CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB; ATTACH DATABASE $DB;"; }

run_as()
{
    local user=$1
    shift
    $CLICKHOUSE_CLIENT --user "$user" --password password --enable_lightweight_update 1 --query "$*" 2>&1 \
        | grep -o -m1 "ACCESS_DENIED" || echo "OK"
}

echo "--- only ALTER UPDATE"
reattach
run_as "$user_upd" "UPDATE $DB.t SET _row_exists = 0 WHERE id = 1"
run_as "$user_upd" "UPDATE $DB.t SET v = 100 WHERE id = 2"

echo "--- only ALTER DELETE"
reattach
run_as "$user_del" "UPDATE $DB.t SET _row_exists = 0 WHERE id = 3"
run_as "$user_del" "UPDATE $DB.t SET v = 100 WHERE id = 4"

$CLICKHOUSE_CLIENT --query "SELECT id, v FROM $DB.t WHERE id IN (1, 2, 3, 4) ORDER BY id"

$CLICKHOUSE_CLIENT --query "
DROP USER $user_del, $user_upd;
DROP DATABASE $DB SYNC;
"
