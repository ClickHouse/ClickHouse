#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database
# no-fasttest: the vector similarity index (USearch) is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for `useVectorSearch` with a hidden reader-side filter (here a row policy).
#
# A row policy restricts rows inside the reader, just like a `WHERE` / `PREWHERE`, so it must set
# `additional_filters_present`. Otherwise a query filtered only by a hidden policy was treated as
# unfiltered: the `vector_search_filter_strategy = 'prefilter'` (exact, brute-force) bailout was
# skipped and the query used the vector similarity index anyway, and the index fetched only `LIMIT`
# neighbors without the `vector_search_index_fetch_multiplier` compensation - so a policy such as
# `USING id != 0` could make `ORDER BY cosineDistance(...) LIMIT 1` miss the next allowed neighbor.
# Any non-null `getRowLevelFilter()` now participates in `additional_filters_present`.

${CLICKHOUSE_CLIENT} --query="DROP ROW POLICY IF EXISTS rp_04634 ON t_04634"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_04634"
${CLICKHOUSE_CLIENT} --query="
    CREATE TABLE t_04634 (id UInt32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4"
${CLICKHOUSE_CLIENT} --query="INSERT INTO t_04634 SELECT number, [toFloat32(number), toFloat32(number + 1)] FROM numbers(64)"

# `EXPLAIN indexes = 1` lists the `vector_similarity` skip index only when the optimization engages the
# index. `enable_parallel_replicas = 0`: the optimization is disabled on a distributed plan.
uses_vector_index() {
    ${CLICKHOUSE_CLIENT} --query="EXPLAIN indexes = 1 $1" 2>&1 | grep -c "vector_similarity"
}
tail="ORDER BY cosineDistance(vec, [0.0, 1.0]) LIMIT 1 SETTINGS enable_parallel_replicas = 0"

# Sanity: without a policy, `prefilter` with no additional filter still uses the index.
no_policy=$(uses_vector_index "SELECT id FROM t_04634 $tail, vector_search_filter_strategy = 'prefilter'")

${CLICKHOUSE_CLIENT} --query="CREATE ROW POLICY rp_04634 ON t_04634 FOR SELECT USING id != 0 TO ALL"

# The row policy is now an additional filter, so an explicit request for exact (brute-force) search
# via `vector_search_filter_strategy = 'prefilter'` must fall back off the index.
with_policy_prefilter=$(uses_vector_index "SELECT id FROM t_04634 $tail, vector_search_filter_strategy = 'prefilter'")
# With the default (post-filter) strategy the index is still used - the policy just makes the query
# take the filtered path with fetch-multiplier compensation.
with_policy_postfilter=$(uses_vector_index "SELECT id FROM t_04634 $tail, vector_search_filter_strategy = 'postfilter'")

if [ "$no_policy" -ge 1 ] && [ "$with_policy_prefilter" -eq 0 ] && [ "$with_policy_postfilter" -ge 1 ]; then
    echo "OK"
else
    echo "FAIL no_policy=$no_policy with_policy_prefilter=$with_policy_prefilter with_policy_postfilter=$with_policy_postfilter"
fi

${CLICKHOUSE_CLIENT} --query="DROP ROW POLICY rp_04634 ON t_04634"
${CLICKHOUSE_CLIENT} --query="DROP TABLE t_04634"
