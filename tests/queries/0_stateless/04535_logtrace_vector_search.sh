#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database
# no-fasttest: the vector similarity index (USearch) and the Quantized codec are not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A stateful function (e.g. `logTrace`, `neighbor`, `runningAccumulate`) below an `ORDER BY <distance> LIMIT` must
# observe every input block. The vector-search optimizations rewrite the plan to shortlist a handful of candidate rows
# and only feed those to the expression, which would run the stateful function on the reduced stream:
#   - `useVectorSearch` matches `Limit -> Sorting -> Expression -> [Filter] -> ReadFromMergeTree` and uses the vector
#     similarity index to read only the shortlisted granules;
#   - `useVectorSearchWithQuantizedCodes` shortlists over the quantized codes subcolumn and rescores the survivors.
# The `hasStatefulFunctions()` guards keep both optimizations from firing when a stateful function sits in the
# projection or the WHERE chain (mirroring the guards in `optimizeTopK`, `liftUpFunctions`, and lazy materialization).

# --- useVectorSearch (vector similarity index) ---
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_04535"
${CLICKHOUSE_CLIENT} --query="
    CREATE TABLE t_04535 (id UInt32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
    ENGINE = MergeTree PARTITION BY (id % 4) ORDER BY id SETTINGS index_granularity = 4"
${CLICKHOUSE_CLIENT} --query="INSERT INTO t_04535 SELECT number, [toFloat32(number), toFloat32(number + 1)] FROM numbers(64)"

# `EXPLAIN indexes = 1` lists the `vector_similarity` skip index only when the optimization engages the index.
# `optimize_move_to_prewhere = 0` keeps a WHERE predicate as a `FilterStep` (rather than moving it into the reader) so
# the filter-chain guard is exercised. `enable_parallel_replicas = 0`: the optimization is disabled on a distributed
# plan, which would make even the non-stateful control not use the index.
uses_vector_index() {
    ${CLICKHOUSE_CLIENT} --query="EXPLAIN indexes = 1 $1" 2>&1 | grep -c "vector_similarity"
}
tail="ORDER BY cosineDistance(vec, [0.0, 1.0]) LIMIT 1 SETTINGS enable_parallel_replicas = 0, optimize_move_to_prewhere = 0"
control=$(uses_vector_index "SELECT id FROM t_04535 $tail")
control_where=$(uses_vector_index "SELECT id FROM t_04535 WHERE id != 7 $tail")
lt_select=$(uses_vector_index "SELECT logTrace('vs'), id FROM t_04535 $tail")
lt_where=$(uses_vector_index "SELECT id FROM t_04535 WHERE logTrace('vs') = 0 $tail")
if [ "$control" -ge 1 ] && [ "$control_where" -ge 1 ] && [ "$lt_select" -eq 0 ] && [ "$lt_where" -eq 0 ]; then
    echo "OK"
else
    echo "FAIL control=$control control_where=$control_where lt_select=$lt_select lt_where=$lt_where"
fi

# The per-block behavior actually holds: with the index available, `logTrace` still fires for every block because the
# guard forces the full scan. The table has 4 partitions (never merged), so the read spans > 1 block regardless of the
# harness's randomized `index_granularity` / `max_block_size`.
n=$(${CLICKHOUSE_CLIENT} --send_logs_level=trace --query="
    SELECT logTrace('vscnt'), id FROM t_04535 ORDER BY cosineDistance(vec, [0.0, 1.0]) LIMIT 1
    SETTINGS enable_parallel_replicas = 0, max_threads = 1 FORMAT Null" 2>&1 | grep -c "FunctionLogTrace: vscnt")
if [ "$n" -gt 1 ]; then echo "OK"; else echo "FAIL n=$n"; fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE t_04535"

# --- useVectorSearchWithQuantizedCodes (Quantized-coded column) ---
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_04535_q"
${CLICKHOUSE_CLIENT} --allow_experimental_codecs=1 --query="
    CREATE TABLE t_04535_q (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64)))
    ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query="
    INSERT INTO t_04535_q (id, vec)
    SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)) FROM numbers(4000)"

# The two-stage rewrite adds an inner "quantized shortlist" `LimitStep` to the plan. It is analyzer-only, opt-in via
# `vector_search_use_quantized_codes`, and disabled on a distributed plan.
has_shortlist() {
    ${CLICKHOUSE_CLIENT} --query="EXPLAIN PLAN $1" 2>&1 | grep -ci "quantized shortlist"
}
qtail="ORDER BY cosineDistance(vec, (SELECT vec FROM t_04535_q WHERE id = 123)) ASC LIMIT 5
    SETTINGS vector_search_use_quantized_codes = 1, enable_analyzer = 1, enable_parallel_replicas = 0,
             vector_search_index_fetch_multiplier = 50,
             query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 1000000"
q_control=$(has_shortlist "SELECT id FROM t_04535_q $qtail")
q_lt=$(has_shortlist "SELECT logTrace('q'), id FROM t_04535_q $qtail")
if [ "$q_control" -ge 1 ] && [ "$q_lt" -eq 0 ]; then echo "OK"; else echo "FAIL q_control=$q_control q_lt=$q_lt"; fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE t_04535_q"
