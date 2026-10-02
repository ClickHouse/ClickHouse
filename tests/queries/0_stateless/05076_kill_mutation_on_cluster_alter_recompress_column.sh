#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `KILL MUTATION` of a queued `RECOMPRESS COLUMN` mutation must be governed by the `ALTER RECOMPRESS COLUMN`
# privilege on both entry points:
#  - the local `KILL MUTATION` checks, per matched mutation, the privilege the mutation's own `ALTER` command
#    requires (`InterpreterAlterQuery::getRequiredAccessForCommand`), i.e. `ALTER RECOMPRESS COLUMN(col) ON db.table`;
#  - `KILL MUTATION ON CLUSTER` is checked on the initiator before the task is enqueued. The initiator does not
#    know which mutations the `WHERE` will match on the hosts, so it requires the union of the privileges of
#    every mutation type, globally (`InterpreterKillQueryQuery::getRequiredAccessForDDLOnCluster`). That union
#    must include `ALTER RECOMPRESS COLUMN`: a user holding every other privilege of the union is rejected, and
#    the rejection names the missing grant.

table="t_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_local_other="u_local_other_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_local_recompress="u_local_recompress_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_cluster_without="u_cluster_without_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_cluster_with="u_cluster_with_${CLICKHOUSE_TEST_UNIQUE_NAME}"
cluster="test_shard_localhost"

# Every mutation privilege of the `KILL MUTATION ON CLUSTER` allowlist except `ALTER RECOMPRESS COLUMN`.
other_mutation_privileges="ALTER UPDATE, ALTER DELETE, ALTER MATERIALIZE INDEX, ALTER MATERIALIZE COLUMN, ALTER MATERIALIZE TTL, ALTER REWRITE PARTS"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_local_other}, ${user_local_recompress}, ${user_cluster_without}, ${user_cluster_with}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${table} (id UInt64, s String CODEC(ZSTD)) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${table} SELECT number, toString(number) FROM numbers(1000)"

# `KILL MUTATION` reads `system.mutations` under the calling user, so every user gets that grant.
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_local_other}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON system.mutations TO ${user_local_other}"
# Other privileges of the same table, but not the one the queued command requires.
${CLICKHOUSE_CLIENT} --query "GRANT ALTER UPDATE, ALTER MODIFY COLUMN ON ${table} TO ${user_local_other}"

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_local_recompress}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON system.mutations TO ${user_local_recompress}"
# Only the column-level privilege of the queued command.
${CLICKHOUSE_CLIENT} --query "GRANT ALTER RECOMPRESS COLUMN(s) ON ${table} TO ${user_local_recompress}"

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_cluster_without}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON system.mutations TO ${user_cluster_without}"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER, ${other_mutation_privileges} ON *.* TO ${user_cluster_without}"

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_cluster_with}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON system.mutations TO ${user_cluster_with}"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER, ${other_mutation_privileges}, ALTER RECOMPRESS COLUMN ON *.* TO ${user_cluster_with}"

# Keep the mutation queued so that there is something to kill.
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES ${table}"

queue_recompression()
{
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${table} RECOMPRESS COLUMN s"
    ${CLICKHOUSE_CLIENT} --query "SELECT 'queued', count() FROM system.mutations WHERE database = currentDatabase() AND table = '${table}' AND NOT is_done"
}

pending_count()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT 'pending', count() FROM system.mutations WHERE database = currentDatabase() AND table = '${table}' AND NOT is_done"
}

echo "-- local KILL MUTATION"
queue_recompression

echo "without ALTER RECOMPRESS COLUMN on the table:"
${CLICKHOUSE_CLIENT} --user "${user_local_other}" --query "KILL MUTATION WHERE database = currentDatabase() AND table = '${table}'" 2>&1 | grep -o -m1 "ACCESS_DENIED"
pending_count

echo "with ALTER RECOMPRESS COLUMN(s) on the table:"
${CLICKHOUSE_CLIENT} --user "${user_local_recompress}" --query "KILL MUTATION WHERE database = currentDatabase() AND table = '${table}'" | cut -f1,5
pending_count

echo "-- KILL MUTATION ON CLUSTER"
queue_recompression

echo "with every other mutation privilege, but not ALTER RECOMPRESS COLUMN:"
${CLICKHOUSE_CLIENT} --user "${user_cluster_without}" --distributed_ddl_output_mode throw --query "KILL MUTATION ON CLUSTER ${cluster} WHERE database = currentDatabase() AND table = '${table}'" 2>&1 | grep -o -E "ACCESS_DENIED|ALTER RECOMPRESS COLUMN" | sort -u
pending_count

echo "with ALTER RECOMPRESS COLUMN as well:"
${CLICKHOUSE_CLIENT} --user "${user_cluster_with}" --distributed_ddl_output_mode none --query "KILL MUTATION ON CLUSTER ${cluster} WHERE database = currentDatabase() AND table = '${table}'"
pending_count

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES ${table}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user_local_other}, ${user_local_recompress}, ${user_cluster_without}, ${user_cluster_with}"
