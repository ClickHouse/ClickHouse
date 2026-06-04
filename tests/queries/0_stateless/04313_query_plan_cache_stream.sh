#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: messes with internal caches and system.query_log

# Test: a `SELECT ... FROM t STREAM` query must not be admitted to the query plan
# cache. The STREAM cursor state is not part of the serialized plan, so caching it
# would materialize a streaming read as a normal non-streaming one. The admission
# path rejects it, so neither a hit nor a miss is recorded for such a query.

set -eo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CURDIR"/streaming.lib

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_plan_cache_stream"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_plan_cache_stream (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_plan_cache_stream VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY PLAN CACHE"

settings="--enable_streaming_queries=1 --use_skip_indexes_on_data_read=0 \
    --allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 \
    --allow_experimental_analyzer=1 --enable_parallel_replicas=0"

# Bounded STREAM read returns the existing rows and finishes. Run it twice: if it
# were cached, the second run would report a hit.
echo "stream output:"
${CLICKHOUSE_CLIENT} --user default $settings --query "SELECT a FROM t_plan_cache_stream STREAM LIMIT 3" | sort
${CLICKHOUSE_CLIENT} --user default $settings --query "SELECT a FROM t_plan_cache_stream STREAM LIMIT 3" >/dev/null

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
echo "hits / misses (must be 0 0, not admitted):"
${CLICKHOUSE_CLIENT} --query "
    SELECT
        sum(ProfileEvents['QueryPlanCacheHits']) AS hits,
        sum(ProfileEvents['QueryPlanCacheMisses']) AS misses
    FROM system.query_log
    WHERE event_date >= yesterday()
      AND type = 'QueryFinish'
      AND current_database = currentDatabase()
      AND query LIKE 'SELECT a FROM t_plan_cache_stream STREAM%'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_plan_cache_stream"
