#!/usr/bin/env bash

# `ALTER TABLE ... ATTACH PARTITION ... FROM ...` and `CREATE TABLE ... CLONE AS ...` only add data to
# the destination table, so they must require only `INSERT`, not `ALTER DELETE`. `REPLACE PARTITION`
# does drop the data currently in the destination partition, so it still requires `ALTER DELETE`.
# https://github.com/ClickHouse/ClickHouse/issues/90834

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_ins="${CLICKHOUSE_DATABASE}_ins_05061"
user_del="${CLICKHOUSE_DATABASE}_del_05061"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS src, dst;
DROP USER IF EXISTS $user_ins, $user_del;

CREATE TABLE src (id UInt32, val UInt32) ENGINE = MergeTree PARTITION BY id ORDER BY id;
INSERT INTO src SELECT number, number FROM numbers(3);

CREATE TABLE dst (id UInt32, val UInt32) ENGINE = MergeTree PARTITION BY id ORDER BY id;

-- Can read the source and write to the destination, but cannot delete from the destination.
CREATE USER $user_ins IDENTIFIED WITH plaintext_password BY 'password';
GRANT SELECT ON $CLICKHOUSE_DATABASE.src TO $user_ins;
GRANT INSERT ON $CLICKHOUSE_DATABASE.dst TO $user_ins;
GRANT CREATE TABLE, INSERT ON $CLICKHOUSE_DATABASE.cloned TO $user_ins;

-- Can read the source and delete from the destination, but cannot write to the destination.
CREATE USER $user_del IDENTIFIED WITH plaintext_password BY 'password';
GRANT SELECT ON $CLICKHOUSE_DATABASE.src TO $user_del;
GRANT ALTER DELETE ON $CLICKHOUSE_DATABASE.dst TO $user_del;
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

echo "-- ATTACH PARTITION FROM needs only INSERT on the destination"
check_access "$user_ins" "ALTER TABLE dst ATTACH PARTITION 1 FROM src"
check_access "$user_ins" "ALTER TABLE dst ATTACH PARTITION ALL FROM src"

echo "-- ... and INSERT is still required: ALTER DELETE alone is not enough"
check_access "$user_del" "ALTER TABLE dst ATTACH PARTITION 1 FROM src"

echo "-- CREATE TABLE ... CLONE AS attaches all partitions, so it needs only INSERT as well"
check_access "$user_ins" "CREATE TABLE cloned CLONE AS src"

echo "-- REPLACE PARTITION drops the destination data, so it still needs ALTER DELETE"
check_access "$user_ins" "ALTER TABLE dst REPLACE PARTITION 1 FROM src"

$CLICKHOUSE_CLIENT -q "GRANT ALTER DELETE ON $CLICKHOUSE_DATABASE.dst TO $user_ins"

echo "-- ... and works once ALTER DELETE is granted"
check_access "$user_ins" "ALTER TABLE dst REPLACE PARTITION 1 FROM src"

echo "-- the cloned table has the data of the source"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM cloned"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS src, dst, cloned;
DROP USER IF EXISTS $user_ins, $user_del;
"
