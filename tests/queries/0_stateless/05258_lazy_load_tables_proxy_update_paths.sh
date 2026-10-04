#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table of a database with `lazy_load_tables` is kept in the catalog as a stand-in for the real
# storage. Lightweight `UPDATE` used to pass the capability check and then fail on the stand-in, and
# `ALTER ... UPDATE` was validated without the MergeTree settings of the table.

DB="${CLICKHOUSE_DATABASE}_lazy"

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DB SYNC;
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE $DB.lwu (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO $DB.lwu SELECT number, number FROM numbers(10);

CREATE TABLE $DB.idx (a UInt64, b UInt64, INDEX idx_minmax b TYPE minmax) ENGINE = MergeTree ORDER BY a
    SETTINGS alter_column_secondary_index_mode = 'throw';
INSERT INTO $DB.idx SELECT number, number FROM numbers(10);
"

# Reloading the database installs the stand-in, as a server restart does.
reattach() { $CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB; ATTACH DATABASE $DB;"; }

echo "--- lightweight UPDATE"
reattach
$CLICKHOUSE_CLIENT --query "UPDATE $DB.lwu SET v = 100 WHERE id = 1 SETTINGS enable_lightweight_update = 1"
$CLICKHOUSE_CLIENT --query "SELECT v FROM $DB.lwu WHERE id = 1"

echo "--- lightweight DELETE through UPDATE"
reattach
$CLICKHOUSE_CLIENT --query "DELETE FROM $DB.lwu WHERE id = 2 SETTINGS enable_lightweight_update = 1, lightweight_delete_mode = 'lightweight_update_force'"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM $DB.lwu"

echo "--- ALTER UPDATE respects alter_column_secondary_index_mode"
reattach
$CLICKHOUSE_CLIENT --query "ALTER TABLE $DB.idx UPDATE b = 100 WHERE a = 1 SETTINGS mutations_sync = 2" 2>&1 | grep -o -m1 "SUPPORT_IS_DISABLED"
$CLICKHOUSE_CLIENT --query "SELECT b FROM $DB.idx WHERE a = 1"

$CLICKHOUSE_CLIENT --query "DROP DATABASE $DB SYNC"
