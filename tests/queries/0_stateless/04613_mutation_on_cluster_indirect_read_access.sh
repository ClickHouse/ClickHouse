#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database

# An `ON CLUSTER` mutation is enqueued for a DDL worker that does not run as the initiating user
# (unless `distributed_ddl_use_initial_user_and_roles` is enabled), so access to what the mutation
# reads indirectly - through a subquery, a table on the right of `IN`, `dictGet`, `joinGet` or a SQL
# UDF body - has to be required on the initiator, before the enqueue, and independently of
# `validate_mutation_query`.
# no-replicated-database: ON CLUSTER is disallowed in replicated-database stateless runs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cluster="test_shard_localhost"
user_name="${CLICKHOUSE_DATABASE}_user_04613"
udf_name="${CLICKHOUSE_DATABASE}_leak_04613"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab, secret_tab;
DROP FUNCTION IF EXISTS $udf_name;
DROP USER IF EXISTS $user_name;

CREATE TABLE tab (id UInt32, name String, hidden UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO tab VALUES (1, 'a', 7), (42, 'b', 8);

CREATE TABLE secret_tab (secret UInt32, payload String) ENGINE = MergeTree ORDER BY secret;
INSERT INTO secret_tab VALUES (42, 'TOP-SECRET');

CREATE FUNCTION $udf_name AS () -> hidden = 7;

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
GRANT CLUSTER ON *.* TO $user_name;
GRANT ALTER UPDATE, ALTER DELETE, UPDATE, DELETE ON $CLICKHOUSE_DATABASE.tab TO $user_name;
-- The user can read and write 'id' and 'name', but not 'hidden', and has no grant on 'secret_tab'.
GRANT SELECT(id, name) ON $CLICKHOUSE_DATABASE.tab TO $user_name;
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

off="validate_mutation_query = 0, distributed_ddl_task_timeout = 60"

echo "-- ON CLUSTER, reads hidden behind a subquery, validation off"
check_access "ALTER TABLE tab ON CLUSTER $cluster DELETE WHERE id IN (SELECT secret FROM secret_tab) SETTINGS $off"
check_access "ALTER TABLE tab ON CLUSTER $cluster UPDATE name = (SELECT max(payload) FROM secret_tab) WHERE 1 SETTINGS $off"
check_access "DELETE FROM tab ON CLUSTER $cluster WHERE id IN (SELECT secret FROM secret_tab) SETTINGS $off"

echo "-- ON CLUSTER, a UDF body reading an unreadable column, validation off"
check_access "ALTER TABLE tab ON CLUSTER $cluster DELETE WHERE $udf_name() SETTINGS $off"
check_access "DELETE FROM tab ON CLUSTER $cluster WHERE $udf_name() SETTINGS $off"
check_access "UPDATE tab ON CLUSTER $cluster SET name = '' WHERE $udf_name() SETTINGS $off, enable_lightweight_update = 1"

$CLICKHOUSE_CLIENT -q "
GRANT SELECT ON $CLICKHOUSE_DATABASE.secret_tab TO $user_name;
GRANT SELECT(hidden) ON $CLICKHOUSE_DATABASE.tab TO $user_name;
"

echo "-- With the grants, the same ON CLUSTER mutations are allowed"
# The predicates keep every allowed mutation a no-op, so the secret is never actually copied.
check_access "ALTER TABLE tab ON CLUSTER $cluster DELETE WHERE id IN (SELECT secret FROM secret_tab) AND 0 SETTINGS $off"
check_access "ALTER TABLE tab ON CLUSTER $cluster UPDATE name = (SELECT max(payload) FROM secret_tab) WHERE 0 SETTINGS $off"
check_access "ALTER TABLE tab ON CLUSTER $cluster DELETE WHERE $udf_name() AND 0 SETTINGS $off"

echo "-- The value of the unreadable table never reached a readable column"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM tab WHERE name = 'TOP-SECRET'"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS tab, secret_tab;
DROP FUNCTION IF EXISTS $udf_name;
DROP USER IF EXISTS $user_name;
"
