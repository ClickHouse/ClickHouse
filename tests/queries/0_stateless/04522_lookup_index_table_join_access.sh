#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"

${CLICKHOUSE_CLIENT} -nm --query "
    SET allow_experimental_lookup_index = 1;

    CREATE TABLE ${db}.lookup_join_acc_dim
    (
        id UInt64,
        val String,
        secret String,
        LOOKUP INDEX idx_join (id) TYPE table_join
    )
    ENGINE = MergeTree
    ORDER BY id;

    CREATE TABLE ${db}.lookup_join_acc_fact
    (
        id UInt64,
        payload String
    )
    ENGINE = MergeTree
    ORDER BY id;

    INSERT INTO ${db}.lookup_join_acc_dim VALUES (1, 'a', 's1'), (2, 'b', 's2');
    INSERT INTO ${db}.lookup_join_acc_fact VALUES (1, 'x'), (2, 'y');

    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${db}.lookup_join_acc_fact TO ${user};
    GRANT SELECT(id, val) ON ${db}.lookup_join_acc_dim TO ${user};
"

# The user can read the fact table and the dimension's join key `id` and payload `val`, but has no
# SELECT on the dimension's `secret` column. The `table_join` lookup fast path builds its direct-join
# entity from *all* physical columns (`getAllPhysicalColumnsForLookupJoin`), so it would read and cache
# `secret` even for a join that only requests `id` and `val`. It must therefore fall back to the regular
# join path (which reads and access-checks only the requested columns) unless the user is granted SELECT
# on every physical column of the dimension table.
#
# The fast path only fires when no join size limit is active, so `max_rows_in_join` / `max_bytes_in_join`
# are reset to 0 here (the standard test config sets them to a high non-zero ceiling, which would
# otherwise make the fast path decline for an unrelated reason).

client_opts="--user=${user}"
join_settings="allow_experimental_lookup_index = 1, join_algorithm = 'direct,hash', max_rows_in_join = 0, max_bytes_in_join = 0, enable_parallel_replicas = 0, max_parallel_replicas = 1, serialize_query_plan = 0"

echo 'join of allowed columns succeeds (regular path):'
${CLICKHOUSE_CLIENT} ${client_opts} --query "
    SELECT f.id, d.val
    FROM ${db}.lookup_join_acc_fact AS f
    INNER ALL JOIN ${db}.lookup_join_acc_dim AS d USING (id)
    ORDER BY f.id
    SETTINGS ${join_settings};
"

# Observe the plan with a plain EXPLAIN (grepped in the shell) rather than `SELECT ... FROM (EXPLAIN ...)`,
# which would require the restricted user to hold `CREATE TEMPORARY TABLE`.
echo 'no SELECT on secret: DirectKeyValueJoin declined:'
${CLICKHOUSE_CLIENT} ${client_opts} --query "
    EXPLAIN PLAN actions = 1
    SELECT f.id, d.val
    FROM ${db}.lookup_join_acc_fact AS f
    INNER ALL JOIN ${db}.lookup_join_acc_dim AS d USING (id)
    SETTINGS ${join_settings};
" | grep -c 'Algorithm: DirectKeyValueJoin'

echo 'reading the denied secret column is still ACCESS_DENIED:'
${CLICKHOUSE_CLIENT} ${client_opts} --query "
    SELECT f.id, d.secret
    FROM ${db}.lookup_join_acc_fact AS f
    INNER ALL JOIN ${db}.lookup_join_acc_dim AS d USING (id)
    ORDER BY f.id
    SETTINGS ${join_settings}; -- { serverError ACCESS_DENIED }
"

# After granting SELECT on the whole dimension table, the lookup fast path may fire.
${CLICKHOUSE_CLIENT} -nm --query "GRANT SELECT ON ${db}.lookup_join_acc_dim TO ${user};"

echo 'full access: join result unchanged:'
${CLICKHOUSE_CLIENT} ${client_opts} --query "
    SELECT f.id, d.val
    FROM ${db}.lookup_join_acc_fact AS f
    INNER ALL JOIN ${db}.lookup_join_acc_dim AS d USING (id)
    ORDER BY f.id
    SETTINGS ${join_settings};
"

echo 'full access: DirectKeyValueJoin used:'
${CLICKHOUSE_CLIENT} ${client_opts} --query "
    EXPLAIN PLAN actions = 1
    SELECT f.id, d.val
    FROM ${db}.lookup_join_acc_fact AS f
    INNER ALL JOIN ${db}.lookup_join_acc_dim AS d USING (id)
    SETTINGS ${join_settings};
" | grep -c 'Algorithm: DirectKeyValueJoin'

${CLICKHOUSE_CLIENT} -nm --query "
    DROP USER IF EXISTS ${user};
    DROP TABLE IF EXISTS ${db}.lookup_join_acc_dim SYNC;
    DROP TABLE IF EXISTS ${db}.lookup_join_acc_fact SYNC;
"
