#!/usr/bin/env bash

# Tests that ALTER UPDATE/DELETE and lightweight UPDATE/DELETE require SELECT access
# on columns referenced in the WHERE predicate (https://github.com/ClickHouse/ClickHouse/issues/105614).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_user_04327"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab;
DROP USER IF EXISTS $user_name;

CREATE TABLE tab (id UInt32, name String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO tab VALUES (1, 'a'), (42, 'b');

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
GRANT ALTER UPDATE, ALTER DELETE ON $CLICKHOUSE_DATABASE.tab TO $user_name;
GRANT UPDATE, DELETE ON $CLICKHOUSE_DATABASE.tab TO $user_name;
-- The user can write the 'name' column but cannot read the 'id' column.
GRANT SELECT(name) ON $CLICKHOUSE_DATABASE.tab TO $user_name;
"

function check_access()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1" 2>&1)
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo "OK"
    elif echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "$output"
    fi
}

echo "-- Without SELECT(id): WHERE on id must be denied"
check_access "ALTER TABLE tab DELETE WHERE id = 42"
check_access "ALTER TABLE tab UPDATE name = '' WHERE id = 1"
check_access "DELETE FROM tab WHERE id = 42"
check_access "UPDATE tab SET name = '' WHERE id = 1 SETTINGS enable_lightweight_update = 1"

echo "-- WHERE on a readable column (name) is allowed"
check_access "ALTER TABLE tab DELETE WHERE name = 'x'"
check_access "ALTER TABLE tab UPDATE name = '' WHERE name = 'y'"

$CLICKHOUSE_CLIENT -q "GRANT SELECT(id) ON $CLICKHOUSE_DATABASE.tab TO $user_name;"

echo "-- With SELECT(id): WHERE on id is allowed"
check_access "ALTER TABLE tab DELETE WHERE id = 42"
check_access "ALTER TABLE tab UPDATE name = '' WHERE id = 1"
check_access "DELETE FROM tab WHERE id = 42"
check_access "UPDATE tab SET name = '' WHERE id = 1 SETTINGS enable_lightweight_update = 1"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab;
DROP USER IF EXISTS $user_name;
"
