#!/usr/bin/env bash
# Tags: no-fasttest
#     no-fasttest: Produces wrong results in fasttest, unclear why, didn't reproduce locally.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --query_cache_tag $CLICKHOUSE_TEST_UNIQUE_NAME"

# Start with an empty query cache (QC). The clear is scoped by `TAG` to this test's entries:
# an unscoped `SYSTEM CLEAR QUERY CACHE` is server-wide and would drop the entries of every
# other test running at the same time, and this test's own assertions only ever count entries
# carrying its tag, so the scoped form is equivalent here.
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE TAG '$CLICKHOUSE_TEST_UNIQUE_NAME'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tab"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE tab (a UInt64) ENGINE=MergeTree() ORDER BY a"
${CLICKHOUSE_CLIENT} --query "INSERT INTO tab VALUES (1) (2) (3)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO tab VALUES (3) (4) (5)"

SETTINGS_ANALYZER="SETTINGS use_query_cache=1, max_threads=1, enable_analyzer=1, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0.0, optimize_trivial_count_query=1"

# Verify that the first query does two aggregations and the second query zero aggregations. Since query cache is currently not integrated
# with EXPLAIN PLAN, we need to check the logs.
${CLICKHOUSE_CLIENT} --allow_repeated_settings --send_logs_level=trace --query "SELECT count(a) / (SELECT sum(a) FROM tab) FROM tab $SETTINGS_ANALYZER" 2>&1 | grep "Aggregated. " | wc -l
${CLICKHOUSE_CLIENT} --allow_repeated_settings --send_logs_level=trace --query "SELECT count(a) / (SELECT sum(a) FROM tab) FROM tab $SETTINGS_ANALYZER" 2>&1 | grep "Aggregated. " | wc -l

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE TAG '$CLICKHOUSE_TEST_UNIQUE_NAME'"
