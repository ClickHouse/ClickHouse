#!/usr/bin/env bash
# Tags: no-parallel
# Test: query plan cache respects column-level access rights revocation.

set -eo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="plan_cache_column_acl_user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_plan_cache_column_acl"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_plan_cache_column_acl (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_plan_cache_column_acl VALUES (1, 10), (2, 20)"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(a), SELECT(b) ON ${CLICKHOUSE_DATABASE}.t_plan_cache_column_acl TO ${user}"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY PLAN CACHE"

# First query as the test user: populates the cache (miss)
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT a FROM t_plan_cache_column_acl FORMAT Null" \
    --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --allow_experimental_analyzer=1

# Revoke access only to the queried column. Access to another column remains.
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(a) ON ${CLICKHOUSE_DATABASE}.t_plan_cache_column_acl FROM ${user}"

# Second query as the test user: cache hit path must revalidate column-level access and fail.
# Buffer client output before grepping to avoid SIGPIPE: `grep -m1` closes the pipe
# while clickhouse-client is still writing log lines to stderr, which trips pipefail.
output=$(${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT a FROM t_plan_cache_column_acl FORMAT Null" \
    --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --allow_experimental_analyzer=1 2>&1 || true)
echo "$output" | grep -o 'ACCESS_DENIED' | head -n1

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_plan_cache_column_acl"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
