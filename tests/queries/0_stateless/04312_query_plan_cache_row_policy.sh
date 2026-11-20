#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: uses the global query plan cache and system.query_log.

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

suffix="${CLICKHOUSE_DATABASE//[^a-zA-Z0-9_]/_}"
user="qpc_policy_user_${suffix}"
role="qpc_policy_role_${suffix}"
table="qpc_policy_table_${suffix}"
test_start_table="qpc_policy_test_start_${suffix}"
policy_true="qpc_policy_true_${suffix}"
policy_filter="qpc_policy_filter_${suffix}"
query_id_first="qpc_policy_first_${suffix}"
query_id_second="qpc_policy_second_${suffix}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS ${policy_true} ON ${CLICKHOUSE_DATABASE}.${table}"
    ${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS ${policy_filter} ON ${CLICKHOUSE_DATABASE}.${table}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${test_start_table}"
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
    ${CLICKHOUSE_CLIENT} --query "DROP ROLE IF EXISTS ${role}"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --query "CREATE ROLE ${role}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} DEFAULT ROLE ${role}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${test_start_table} (ts DateTime64(6)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${test_start_table} VALUES (now64(6))"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${table} (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${table} VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.${table} TO ${role}"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY ${policy_true} ON ${CLICKHOUSE_DATABASE}.${table} AS RESTRICTIVE USING 1 TO ${role}"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY ${policy_filter} ON ${CLICKHOUSE_DATABASE}.${table} AS RESTRICTIVE USING a = 1 TO ${role}"

settings="--enable_query_plan_cache=1 --allow_experimental_analyzer=1 --enable_parallel_replicas=0"

${CLICKHOUSE_CLIENT} --user "${user}" --query_id "${query_id_first}" ${settings} --query "SELECT a FROM ${table} ORDER BY a"
${CLICKHOUSE_CLIENT} --user "${user}" --query_id "${query_id_second}" ${settings} --query "SELECT a FROM ${table} ORDER BY a"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT
        if(query_id = '${query_id_first}', 'first', 'second') AS execution,
        ProfileEvents['QueryPlanCacheHits'],
        ProfileEvents['QueryPlanCacheMisses'],
        ProfileEvents['QueryPlanCacheValidationMisses']
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND current_database = currentDatabase()
      AND event_time_microseconds >= (SELECT ts FROM ${test_start_table})
      AND query_id IN ('${query_id_first}', '${query_id_second}')
    ORDER BY query_id"
