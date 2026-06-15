#!/usr/bin/env bash
# Tags: no-fasttest
# On a Replicated database, ALTER UPDATE/DELETE must enforce SELECT on the WHERE-predicate columns
# on the initiator, before the DDL is enqueued (the enqueue does not preserve the initiator user).
# https://github.com/ClickHouse/ClickHouse/issues/105614

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="${CLICKHOUSE_DATABASE}_user_04328"
rdb="${CLICKHOUSE_DATABASE}_rdb_04328"

$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "
DROP DATABASE IF EXISTS $rdb;
DROP USER IF EXISTS $user_name;

CREATE DATABASE $rdb ENGINE = Replicated('/test/$CLICKHOUSE_DATABASE/rdb_04328', 's1', 'r1');
CREATE TABLE $rdb.tab (id UInt32, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO $rdb.tab VALUES (1, 'a'), (42, 'b');

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
GRANT ALTER UPDATE, ALTER DELETE ON $rdb.tab TO $user_name;
-- Can write 'name' but cannot read 'id'.
GRANT SELECT(name) ON $rdb.tab TO $user_name;
"

function check_access()
{
    local output
    output=$($CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none --user "$user_name" --password "password" -q "$1" 2>&1)
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo "OK"
    elif echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "$output"
    fi
}

echo "-- Without SELECT(id): WHERE on id is denied on the initiator (before enqueue)"
check_access "ALTER TABLE $rdb.tab DELETE WHERE id = 42"
check_access "ALTER TABLE $rdb.tab UPDATE name = '' WHERE id = 1"

$CLICKHOUSE_CLIENT -q "GRANT SELECT(id) ON $rdb.tab TO $user_name;"

echo "-- With SELECT(id): allowed"
check_access "ALTER TABLE $rdb.tab DELETE WHERE id = 42"
check_access "ALTER TABLE $rdb.tab UPDATE name = '' WHERE id = 1"

$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "
DROP DATABASE IF EXISTS $rdb;
DROP USER IF EXISTS $user_name;
"
