#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree
# (A temporary table is always a plain `MergeTree`, and `SharedMergeTree` cannot attach partitions from it.)

# `ALTER TABLE ... ATTACH/REPLACE PARTITION ... FROM ...` accepts a session temporary table as the source.
# The access check has to resolve the source the same way the execution does: a temporary table lives in
# `_temporary_and_external_tables` and is always readable by its session, so the query must not demand
# `SELECT` on a same-named table of the current database.
# https://github.com/ClickHouse/ClickHouse/issues/90834

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="${CLICKHOUSE_DATABASE}_user_05228"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS dst;
DROP USER IF EXISTS $user;

CREATE TABLE dst (id UInt32, val UInt32) ENGINE = MergeTree PARTITION BY id ORDER BY id;

-- Can create MergeTree temporary tables and write to the destination, but has no SELECT grant on anything.
CREATE USER $user IDENTIFIED WITH plaintext_password BY 'password';
GRANT CREATE ARBITRARY TEMPORARY TABLE ON *.* TO $user;
GRANT TABLE ENGINE ON MergeTree TO $user;
GRANT INSERT ON $CLICKHOUSE_DATABASE.dst TO $user;
"

function run_session()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$user" --password "password" -q "$1" 2>&1)
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo "OK"
    elif echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "$output"
    fi
}

echo "-- ATTACH PARTITION FROM a temporary table needs only INSERT on the destination"
run_session "
CREATE TEMPORARY TABLE src (id UInt32, val UInt32) ENGINE = MergeTree PARTITION BY id ORDER BY id;
INSERT INTO src SELECT number, number FROM numbers(3);
ALTER TABLE dst ATTACH PARTITION 1 FROM src;
ALTER TABLE dst ATTACH PARTITION ALL FROM src;
"

echo "-- REPLACE PARTITION FROM a temporary table still needs ALTER DELETE on the destination"
run_session "
CREATE TEMPORARY TABLE src (id UInt32, val UInt32) ENGINE = MergeTree PARTITION BY id ORDER BY id;
INSERT INTO src SELECT number, number + 100 FROM numbers(3);
ALTER TABLE dst REPLACE PARTITION 1 FROM src;
"

$CLICKHOUSE_CLIENT -q "GRANT ALTER DELETE ON $CLICKHOUSE_DATABASE.dst TO $user"

echo "-- ... and works once ALTER DELETE is granted, without any SELECT grant"
run_session "
CREATE TEMPORARY TABLE src (id UInt32, val UInt32) ENGINE = MergeTree PARTITION BY id ORDER BY id;
INSERT INTO src SELECT number, number + 100 FROM numbers(3);
ALTER TABLE dst REPLACE PARTITION 1 FROM src;
"

echo "-- the destination has the attached rows: partition 1 twice, then replaced by a single row"
$CLICKHOUSE_CLIENT -q "SELECT id, val FROM dst ORDER BY id, val"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS dst;
DROP USER IF EXISTS $user;
"
