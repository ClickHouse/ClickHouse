#!/usr/bin/env bash

# DELETE FROM (lightweight delete) must work with only the ALTER DELETE privilege,
# even though it is internally rewritten to ALTER ... UPDATE _row_exists = 0.
# https://github.com/ClickHouse/ClickHouse/issues/90754

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_del="${CLICKHOUSE_DATABASE}_del_04328"
user_upd="${CLICKHOUSE_DATABASE}_upd_04328"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab, tab_lw, tab_mem, tab_mem2;
DROP USER IF EXISTS $user_del, $user_upd;

CREATE TABLE tab (id UInt32, val UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab SELECT number, number FROM numbers(10);

-- A table that supports lightweight updates, to exercise the lightweight-update delete mode.
CREATE TABLE tab_lw (id UInt32, val UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO tab_lw SELECT number, number FROM numbers(10);

-- A mutable engine where \`_row_exists\` is an ordinary physical column, not the hidden marker.
CREATE TABLE tab_mem (id UInt32, \`_row_exists\` UInt8) ENGINE = Memory;
INSERT INTO tab_mem SELECT number, 1 FROM numbers(10);

-- A Memory table with no \`_row_exists\` column, to exercise the ADD COLUMN + UPDATE escalation guard:
-- \`_row_exists\` is not the hidden marker here, so adding it and setting it to 0 in one ALTER must not
-- be shortcut to ALTER DELETE.
CREATE TABLE tab_mem2 (id UInt32) ENGINE = Memory;
INSERT INTO tab_mem2 SELECT number FROM numbers(10);

-- One user has only ALTER DELETE, the other has only ALTER UPDATE.
-- Both can read, so the predicate columns are accessible.
CREATE USER $user_del IDENTIFIED WITH plaintext_password BY 'password';
GRANT SELECT, ALTER DELETE ON $CLICKHOUSE_DATABASE.tab TO $user_del;
GRANT SELECT, ALTER DELETE ON $CLICKHOUSE_DATABASE.tab_lw TO $user_del;
GRANT SELECT, ALTER DELETE ON $CLICKHOUSE_DATABASE.tab_mem TO $user_del;
GRANT SELECT, ALTER ADD COLUMN, ALTER DELETE ON $CLICKHOUSE_DATABASE.tab_mem2 TO $user_del;

CREATE USER $user_upd IDENTIFIED WITH plaintext_password BY 'password';
GRANT SELECT, ALTER UPDATE ON $CLICKHOUSE_DATABASE.tab TO $user_upd;
GRANT SELECT, ALTER UPDATE ON $CLICKHOUSE_DATABASE.tab_lw TO $user_upd;
GRANT SELECT, ALTER UPDATE ON $CLICKHOUSE_DATABASE.tab_mem TO $user_upd;
"

function check_access()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$1" --password "password" -q "$2" 2>&1)
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo "OK"
    elif echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "$output"
    fi
}

echo "-- ALTER DELETE user: DELETE FROM and ALTER DELETE work; ALTER UPDATE denied"
check_access "$user_del" "DELETE FROM tab WHERE id = 1"
check_access "$user_del" "ALTER TABLE tab DELETE WHERE id = 2"
check_access "$user_del" "ALTER TABLE tab UPDATE val = 100 WHERE id = 3"

echo "-- ALTER UPDATE user: DELETE FROM and ALTER DELETE denied; ALTER UPDATE works"
check_access "$user_upd" "DELETE FROM tab WHERE id = 4"
check_access "$user_upd" "ALTER TABLE tab DELETE WHERE id = 5"
check_access "$user_upd" "ALTER TABLE tab UPDATE val = 100 WHERE id = 6"

# Lightweight-update delete mode routes through a different interpreter, but the
# privilege requirement must be the same: ALTER DELETE, not ALTER UPDATE.
lw_settings="SETTINGS enable_lightweight_update = 1, lightweight_delete_mode = 'lightweight_update_force'"
echo "-- DELETE FROM in lightweight-update mode also needs only ALTER DELETE"
check_access "$user_del" "DELETE FROM tab_lw WHERE id = 7 $lw_settings"
check_access "$user_upd" "DELETE FROM tab_lw WHERE id = 8 $lw_settings"

# Only `_row_exists = 0` is the delete form (governed by ALTER DELETE). Setting the marker to any
# other value edits the deletion mask (e.g. resurrects rows) and is a real update, so it must still
# require ALTER UPDATE -- otherwise ALTER DELETE alone could modify the mask arbitrarily.
echo "-- _row_exists = 0 is a delete (ALTER DELETE); _row_exists = <non-zero> is an update (ALTER UPDATE) -- ALTER TABLE path"
check_access "$user_del" "ALTER TABLE tab UPDATE _row_exists = 0 WHERE id = 9"
check_access "$user_upd" "ALTER TABLE tab UPDATE _row_exists = 0 WHERE id = 9"
check_access "$user_del" "ALTER TABLE tab UPDATE _row_exists = 1 WHERE id = 9"
check_access "$user_upd" "ALTER TABLE tab UPDATE _row_exists = 1 WHERE id = 9"

# The same value-gated split must hold for the direct UPDATE statement (InterpreterUpdateQuery).
echo "-- same split via the direct UPDATE statement (lightweight update)"
check_access "$user_del" "UPDATE tab_lw SET _row_exists = 0 WHERE id = 9 SETTINGS enable_lightweight_update = 1"
check_access "$user_upd" "UPDATE tab_lw SET _row_exists = 0 WHERE id = 9 SETTINGS enable_lightweight_update = 1"
check_access "$user_del" "UPDATE tab_lw SET _row_exists = 1 WHERE id = 9 SETTINGS enable_lightweight_update = 1"
check_access "$user_upd" "UPDATE tab_lw SET _row_exists = 1 WHERE id = 9 SETTINGS enable_lightweight_update = 1"

# On an engine where `_row_exists` is an ordinary physical column (Memory), it is not the hidden
# lightweight-delete marker, so `_row_exists = 0` is a normal update requiring ALTER UPDATE.
echo "-- physical _row_exists column (Memory): _row_exists = 0 is an ordinary update, needs ALTER UPDATE"
check_access "$user_del" "ALTER TABLE tab_mem UPDATE _row_exists = 0 WHERE id = 1"
check_access "$user_upd" "ALTER TABLE tab_mem UPDATE _row_exists = 0 WHERE id = 1"

# Escalation guard: the marker state is computed once on the pre-ALTER table, so on a Memory table with
# no `_row_exists` a user could `ADD COLUMN _row_exists, UPDATE _row_exists = 0` and edit the freshly
# added physical column. `_row_exists` is not a virtual marker on Memory, so the UPDATE segment stays a
# real update needing ALTER UPDATE; ADD COLUMN alone stays permitted for the ALTER DELETE user.
echo "-- escalation guard (Memory, no _row_exists marker): ADD COLUMN alone OK; ADD COLUMN + UPDATE _row_exists = 0 needs ALTER UPDATE"
check_access "$user_del" "ALTER TABLE tab_mem2 ADD COLUMN \`re_probe\` UInt8 DEFAULT 1"
check_access "$user_del" "ALTER TABLE tab_mem2 ADD COLUMN \`_row_exists\` UInt8 DEFAULT 1, UPDATE \`_row_exists\` = 0 WHERE id = 1"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab, tab_lw, tab_mem, tab_mem2;
DROP USER IF EXISTS $user_del, $user_upd;
"
