#!/usr/bin/env bash
# Tags: no-parallel
# Reason: creates users and quotas, and uses the server-wide part aggregation cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The warmup reads that populate the part aggregation cache are done by
# `populatePartAggregationCache` in its own pulling pipelines, not by the query's own pipeline.
# They must still be charged to the query's quota: otherwise a user could consume the whole scan
# under a tight `READ_ROWS` quota and still get the (now cached) answer.

# The functional-test config (`tests/config/users.d/limits.yaml`) sets `max_rows_to_group_by` and
# read limits, on which the optimization fails closed; pin them to 0 so the cache is exercised
# (as in `04033_part_aggregation_cache`).
CACHE_SETTINGS="allow_experimental_analyzer = 0, allow_experimental_part_aggregation_cache = 1, optimize_aggregation_in_order = 0, enable_memory_bound_merging_of_aggregation_results = 0, max_rows_to_group_by = 0, max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0"

USER_COLD="u04648_cold_${CLICKHOUSE_DATABASE}"
USER_WARM="u04648_warm_${CLICKHOUSE_DATABASE}"
QUOTA_COLD="q04648_cold_${CLICKHOUSE_DATABASE}"
QUOTA_WARM="q04648_warm_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA_COLD}, ${QUOTA_WARM}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER_COLD}, ${USER_WARM}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_part_agg_cache_quota"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_part_agg_cache_quota (k UInt32, g UInt32, v UInt64) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES t_part_agg_cache_quota"
# A single part with far more rows than the quota below allows to read.
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_part_agg_cache_quota SELECT number, number % 2, number FROM numbers(100000)"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_COLD}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_WARM}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_COLD}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_WARM}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA_COLD} FOR INTERVAL 100 YEAR MAX read_rows = 1000 TO ${USER_COLD}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA_WARM} FOR INTERVAL 100 YEAR MAX read_rows = 1000 TO ${USER_WARM}"

# Cold cache: the answer can only be produced by reading all 100000 rows of the part, and that read
# happens in the populator. The quota allows 1000 rows, so the query must fail.
${CLICKHOUSE_CLIENT} -q "SYSTEM DROP PART AGGREGATION CACHE"
${CLICKHOUSE_CLIENT} --user "${USER_COLD}" --send_logs_level=none -q \
    "SELECT g, sum(v) FROM t_part_agg_cache_quota GROUP BY g ORDER BY g SETTINGS ${CACHE_SETTINGS}" 2>&1 | grep -o -m1 'QUOTA_EXCEEDED'

# Warm cache: with the states already cached (warmed here by the unrestricted test user), the query
# reads no rows from the part, so the same tight quota is not exceeded and the result is returned.
# This is what makes the failure above meaningful: it comes from charging the warmup reads, not from
# the cache path being unusable under a quota.
${CLICKHOUSE_CLIENT} -q "SYSTEM DROP PART AGGREGATION CACHE"
${CLICKHOUSE_CLIENT} -q "SELECT g, sum(v) FROM t_part_agg_cache_quota GROUP BY g ORDER BY g SETTINGS ${CACHE_SETTINGS}" > /dev/null
${CLICKHOUSE_CLIENT} --user "${USER_WARM}" --send_logs_level=none -q \
    "SELECT g, sum(v) FROM t_part_agg_cache_quota GROUP BY g ORDER BY g SETTINGS ${CACHE_SETTINGS}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_part_agg_cache_quota"
${CLICKHOUSE_CLIENT} -q "SYSTEM DROP PART AGGREGATION CACHE"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${QUOTA_COLD}, ${QUOTA_WARM}"
${CLICKHOUSE_CLIENT} -q "DROP USER ${USER_COLD}, ${USER_WARM}"
