#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: Messes with internal cache
#     no-fasttest: Produces wrong results in fasttest, unclear why, didn't reproduce locally.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Start with empty query cache (QC).
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tab"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE tab (a UInt64) ENGINE=MergeTree() ORDER BY a"
${CLICKHOUSE_CLIENT} --query "INSERT INTO tab VALUES (1) (2) (3)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO tab VALUES (3) (4) (5)"

# Verify that the first query does two aggregations and the second query zero aggregations. Since query cache is currently not integrated
# with EXPLAIN PLAN, we need need to check the logs.
${CLICKHOUSE_CLIENT} --send_logs_level=trace --query "SELECT count(a) / (SELECT sum(a) FROM tab) FROM tab SETTINGS use_query_cache=1, max_threads=1, allow_experimental_analyzer=0" 2>&1 | grep "Aggregated. " | wc -l
${CLICKHOUSE_CLIENT} --send_logs_level=trace --query "SELECT count(a) / (SELECT sum(a) FROM tab) FROM tab SETTINGS use_query_cache=1, max_threads=1, allow_experimental_analyzer=0" 2>&1 | grep "Aggregated. " | wc -l

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"
