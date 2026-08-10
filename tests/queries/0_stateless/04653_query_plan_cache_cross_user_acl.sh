#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: inspects the global query plan cache through system.query_log.

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

suffix="${CLICKHOUSE_DATABASE//[^a-zA-Z0-9_]/_}"
user_a="qpc_acl_a_${suffix}"
user_b="qpc_acl_b_${suffix}"
table="qpc_acl_${suffix}"
test_start_table="qpc_acl_test_start_${suffix}"
query_id_shared="qpc_acl_shared_${suffix}"
query_id_count="qpc_acl_count_${suffix}"
settings="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --allow_experimental_analyzer=1 --enable_parallel_replicas=0 --optimize_trivial_count_query=0"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${test_start_table}"
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_a}"
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_b}"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_a} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_b} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${test_start_table} (ts DateTime64(6)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${test_start_table} VALUES (now64(6))"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${table} (a UInt64, b UInt64, alias_a UInt64 ALIAS a) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${table} VALUES (1, 10), (2, 20), (3, 30)"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.${table} TO ${user_a}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.${table} TO ${user_b}"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY PLAN CACHE"

shared_query="SELECT a FROM ${table} WHERE a = 2"
${CLICKHOUSE_CLIENT} --user "${user_a}" ${settings} --query "${shared_query}" >/dev/null
${CLICKHOUSE_CLIENT} --user "${user_b}" --query_id "${query_id_shared}" ${settings} --query "${shared_query}"

${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(a) ON ${CLICKHOUSE_DATABASE}.${table} FROM ${user_b}"
if ${CLICKHOUSE_CLIENT} --user "${user_b}" ${settings} --query "${shared_query}" >/dev/null 2>&1; then
    echo "expected access denial"
    exit 1
else
    echo "access denied"
fi

${CLICKHOUSE_CLIENT} --query "GRANT SELECT(b) ON ${CLICKHOUSE_DATABASE}.${table} TO ${user_b}"
count_query="SELECT count() FROM ${table}"
${CLICKHOUSE_CLIENT} --user "${user_a}" ${settings} --query "${count_query}" >/dev/null
${CLICKHOUSE_CLIENT} --user "${user_b}" --query_id "${query_id_count}" ${settings} --query "${count_query}"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT
        if(query_id = '${query_id_shared}', 'shared', 'count') AS execution,
        ProfileEvents['QueryPlanCacheHits'],
        ProfileEvents['QueryPlanCacheMisses'],
        ProfileEvents['QueryPlanCachePreAnalysisHits'],
        ProfileEvents['QueryPlanCacheValidationMisses']
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND current_database = currentDatabase()
      AND event_time_microseconds >= (SELECT ts FROM ${test_start_table})
      AND query_id IN ('${query_id_shared}', '${query_id_count}')
    ORDER BY execution DESC"
