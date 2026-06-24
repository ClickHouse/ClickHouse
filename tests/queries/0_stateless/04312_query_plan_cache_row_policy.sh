#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: messes with internal caches and system.query_log

# Test: a cached query plan must keep enforcing the row policy on a cache hit.
# Regression for the row-level security filter being dropped during plan
# universalization: the first SELECT populates the cache, the second one hits it
# and must still return only the rows allowed by the policy.

set -eo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="plan_cache_rp_user_${CLICKHOUSE_DATABASE}"
policy="plan_cache_rp_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_plan_cache_rp"

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_plan_cache_rp (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_plan_cache_rp VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_plan_cache_rp TO ${user}"
# Only row a = 1 is visible to the test user.
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY ${policy} ON ${CLICKHOUSE_DATABASE}.t_plan_cache_rp USING a = 1 TO ${user}"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY PLAN CACHE"

settings="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --allow_experimental_analyzer=1 --enable_parallel_replicas=0"

# First query (cache miss): populates the cache, must return only row 1.
echo "first (miss):"
${CLICKHOUSE_CLIENT} --user "${user}" $settings --query "SELECT a FROM t_plan_cache_rp ORDER BY a"

# Second identical query (cache hit): the policy must still be enforced.
echo "second (hit):"
${CLICKHOUSE_CLIENT} --user "${user}" $settings --query "SELECT a FROM t_plan_cache_rp ORDER BY a"

# Confirm the second query was actually served from the plan cache.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
echo "cache hit recorded:"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryPlanCacheHits'] > 0
    FROM system.query_log
    WHERE event_date >= yesterday()
      AND type = 'QueryFinish'
      AND current_database = currentDatabase()
      AND query LIKE 'SELECT a FROM t_plan_cache_rp%'
    ORDER BY event_time_microseconds DESC
    LIMIT 1"

${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${policy} ON ${CLICKHOUSE_DATABASE}.t_plan_cache_rp"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_plan_cache_rp"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
