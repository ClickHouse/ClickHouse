#!/usr/bin/env bash
# Tags: no-parallel

# Test: query plan cache respects access rights revocation.
# A cached plan must not bypass checkAccess when SELECT privilege is revoked.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="plan_cache_acl_user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_plan_cache_acl"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_plan_cache_acl (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_plan_cache_acl VALUES (1), (2)"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_plan_cache_acl TO ${user}"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY PLAN CACHE"

# First query as the test user: populates the cache (miss)
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT a FROM t_plan_cache_acl FORMAT Null" \
    --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --allow_experimental_analyzer=1

# Revoke SELECT
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.t_plan_cache_acl FROM ${user}"

# Second query as the test user: cache hit path must revalidate access and fail
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT a FROM t_plan_cache_acl FORMAT Null" \
    --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --allow_experimental_analyzer=1 2>&1 | grep -m1 -o 'ACCESS_DENIED'

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_plan_cache_acl"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
