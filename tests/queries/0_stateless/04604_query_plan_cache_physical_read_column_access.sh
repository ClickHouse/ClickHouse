#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: resets the global query plan cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

user="plan_cache_physical_acl_user_${CLICKHOUSE_DATABASE}"
table="${CLICKHOUSE_DATABASE}.t_query_plan_cache_physical_acl"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}" || true
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}" || true
}

trap cleanup EXIT

cleanup

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${table} (a String, b UInt8) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${table} VALUES ('one', 1), ('two', 2)"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(a), SELECT(b) ON ${table} TO ${user}"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY PLAN CACHE"

${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT count() FROM ${table} FORMAT Null" \
    --enable_query_plan_cache=1 \
    --enable_parallel_replicas=0 --optimize_trivial_count_query=0

${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(b) ON ${table} FROM ${user}"

${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT count() FROM ${table} FORMAT Null" \
    --enable_query_plan_cache=1 \
    --enable_parallel_replicas=0 --optimize_trivial_count_query=0

echo "ACCESS_ALLOWED"
