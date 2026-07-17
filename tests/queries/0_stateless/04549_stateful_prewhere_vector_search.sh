#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database
# no-fasttest: the vector similarity index (USearch) and the Quantized codec are not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A stateful function (e.g. `logTrace`, `neighbor`) in a reader-side filter must observe every input
# block, but both vector-search optimizations shortlist candidate rows below the reader-side filters:
#   - `useVectorSearch` reads only the granules the vector similarity index points at, so an explicit
#     `PREWHERE` predicate runs on the reduced candidate stream (post-filtering);
#   - `useVectorSearchWithQuantizedCodes` runs after `optimizePrewhere`, so a stateful `WHERE` predicate
#     may already be hidden inside the reader where the visible-chain guards cannot see it.
# The reader-side `hasStatefulFunctions` fences keep both optimizations from firing (the query falls
# back to the exact full scan). This complements 04535, which covers the visible projection/WHERE chain.

# --- useVectorSearch (vector similarity index), stateful predicate in an explicit PREWHERE ---
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_04549"
${CLICKHOUSE_CLIENT} --query="
    CREATE TABLE t_04549 (id UInt32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
    ENGINE = MergeTree PARTITION BY (id % 4) ORDER BY id SETTINGS index_granularity = 4"
${CLICKHOUSE_CLIENT} --query="INSERT INTO t_04549 SELECT number, [toFloat32(number), toFloat32(number + 1)] FROM numbers(64)"

# `EXPLAIN indexes = 1` lists the `vector_similarity` skip index only when the optimization engages the
# index. `enable_parallel_replicas = 0`: the optimization is disabled on a distributed plan, which would
# make even the non-stateful control not use the index.
uses_vector_index() {
    ${CLICKHOUSE_CLIENT} --query="EXPLAIN indexes = 1 $1" 2>&1 | grep -c "vector_similarity"
}
tail="ORDER BY cosineDistance(vec, [0.0, 1.0]) LIMIT 1 SETTINGS enable_parallel_replicas = 0"
control=$(uses_vector_index "SELECT id FROM t_04549 PREWHERE id != 7 $tail")
lt_prewhere=$(uses_vector_index "SELECT id FROM t_04549 PREWHERE logTrace('vspw') = 0 $tail")
if [ "$control" -ge 1 ] && [ "$lt_prewhere" -eq 0 ]; then
    echo "OK"
else
    echo "FAIL control=$control lt_prewhere=$lt_prewhere"
fi

# The per-block behavior actually holds: with the index available, the `logTrace` inside the PREWHERE
# still fires for every block because the fence forces the full scan. The table has 4 partitions of 4
# granules each (explicit `index_granularity = 4`, never merged), and `max_block_size = 4` caps a block
# at one granule, so the full scan spans 16 blocks, while the index shortlist would read only about one
# granule per partition (measured: 4 blocks without the fence).
n=$(${CLICKHOUSE_CLIENT} --send_logs_level=trace --query="
    SELECT id FROM t_04549 PREWHERE logTrace('vspwcnt') = 0 ORDER BY cosineDistance(vec, [0.0, 1.0]) LIMIT 1
    SETTINGS enable_parallel_replicas = 0, max_threads = 1, max_block_size = 4 FORMAT Null" 2>&1 | grep -c "FunctionLogTrace: vspwcnt")
if [ "$n" -gt 4 ]; then echo "OK"; else echo "FAIL n=$n"; fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE t_04549"

# --- useVectorSearchWithQuantizedCodes (Quantized-coded column), stateful predicate reader-side ---
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_04549_q"
${CLICKHOUSE_CLIENT} --allow_experimental_codecs=1 --query="
    CREATE TABLE t_04549_q (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64)))
    ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query="
    INSERT INTO t_04549_q (id, vec)
    SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)) FROM numbers(4000)"

# The two-stage rewrite adds an inner "quantized shortlist" `LimitStep` to the plan. The control - a
# deterministic PREWHERE - must keep the rewrite (it prefilters inside the reader), while the stateful
# variants must not: an explicit stateful `PREWHERE` hits the new reader-side fence, and a stateful
# `WHERE` is fenced wherever it lands (moved into the reader by `optimize_move_to_prewhere`, or kept as
# a `FilterStep` in the visible chain).
has_shortlist() {
    ${CLICKHOUSE_CLIENT} --query="EXPLAIN PLAN $1" 2>&1 | grep -ci "quantized shortlist"
}
qtail="ORDER BY cosineDistance(vec, (SELECT vec FROM t_04549_q WHERE id = 123)) ASC LIMIT 5
    SETTINGS vector_search_use_quantized_codes = 1, enable_analyzer = 1, enable_parallel_replicas = 0,
             vector_search_index_fetch_multiplier = 50,
             query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 1000000"
q_control=$(has_shortlist "SELECT id FROM t_04549_q PREWHERE id != 7 $qtail")
q_prewhere=$(has_shortlist "SELECT id FROM t_04549_q PREWHERE logTrace('qpw') = 0 $qtail")
q_where=$(has_shortlist "SELECT id FROM t_04549_q WHERE logTrace('qmv') = 0 $qtail")
if [ "$q_control" -ge 1 ] && [ "$q_prewhere" -eq 0 ] && [ "$q_where" -eq 0 ]; then
    echo "OK"
else
    echo "FAIL q_control=$q_control q_prewhere=$q_prewhere q_where=$q_where"
fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE t_04549_q"
