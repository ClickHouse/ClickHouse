#!/usr/bin/env bash
# Tags: zookeeper, no-parallel

# Regression test for PR #107486: ALTER UPDATE/DELETE, lightweight UPDATE and DELETE FROM issued
# ON CLUSTER must enforce SELECT on the columns read by the WHERE predicate and by UPDATE assignment
# expressions on the *initiating* server, before the query is enqueued for distributed execution. The
# remote DDL worker does not run as the initiator unless distributed_ddl_use_initial_user_and_roles is
# enabled, so without this check the SELECT requirement would be bypassed for ON CLUSTER mutations.
# no-parallel: uses the shared ON CLUSTER distributed DDL queue.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cluster="test_shard_localhost"
user_name="${CLICKHOUSE_DATABASE}_user_04339"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab;
DROP USER IF EXISTS $user_name;

CREATE TABLE tab (id UInt32, name String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO tab VALUES (1, 'a'), (42, 'b');

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
GRANT CLUSTER ON *.* TO $user_name;
GRANT ALTER UPDATE, ALTER DELETE ON $CLICKHOUSE_DATABASE.tab TO $user_name;
GRANT UPDATE, DELETE ON $CLICKHOUSE_DATABASE.tab TO $user_name;
-- The user can read/write 'name' but cannot read 'id'.
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

settings="SETTINGS distributed_ddl_task_timeout = 120, enable_lightweight_update = 1"

echo "-- ON CLUSTER without SELECT(id): reading id via WHERE or the SET right-hand side must be denied"
check_access "ALTER TABLE tab ON CLUSTER $cluster DELETE WHERE id = 42 $settings"
check_access "ALTER TABLE tab ON CLUSTER $cluster UPDATE name = '' WHERE id = 1 $settings"
check_access "ALTER TABLE tab ON CLUSTER $cluster UPDATE name = toString(id) WHERE name = 'a' $settings"
check_access "UPDATE tab ON CLUSTER $cluster SET name = '' WHERE id = 1 $settings"
check_access "DELETE FROM tab ON CLUSTER $cluster WHERE id = 42 $settings"

echo "-- ON CLUSTER reading only the readable column (name) is allowed"
check_access "ALTER TABLE tab ON CLUSTER $cluster DELETE WHERE name = 'x' $settings"

$CLICKHOUSE_CLIENT -q "GRANT SELECT(id) ON $CLICKHOUSE_DATABASE.tab TO $user_name;"

echo "-- ON CLUSTER with SELECT(id): reading id is allowed"
check_access "ALTER TABLE tab ON CLUSTER $cluster UPDATE name = '' WHERE id = 1 $settings"
check_access "UPDATE tab ON CLUSTER $cluster SET name = '' WHERE id = 1 $settings"
check_access "DELETE FROM tab ON CLUSTER $cluster WHERE id = 42 $settings"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab;
DROP USER IF EXISTS $user_name;
"
