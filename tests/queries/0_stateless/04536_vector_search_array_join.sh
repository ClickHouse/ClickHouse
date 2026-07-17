#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database
# no-fasttest: the vector similarity index (USearch) and the Quantized codec are not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `arrayJoin` below an `ORDER BY <distance> LIMIT` changes the number of rows before the sort/limit. The vector-search
# optimizations shortlist a handful of candidate rows and only feed those to the expression/filter chain, so a later
# base row whose expansion should satisfy the LIMIT would be lost (e.g. the nearest base row's array is empty, so
# `arrayJoin` drops it and the query returns too few rows). Both passes must bail out on `arrayJoin`, mirroring the
# `hasArrayJoin` guards in `optimizeTopK`:
#   - `useVectorSearch` matches `Limit -> Sorting -> Expression -> [Filter] -> ReadFromMergeTree` and uses the vector
#     similarity index to read only the shortlisted granules;
#   - `useVectorSearchWithQuantizedCodes` shortlists over the quantized codes subcolumn and rescores the survivors.

# --- useVectorSearch (vector similarity index) ---
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_04536"
${CLICKHOUSE_CLIENT} --allow_experimental_vector_similarity_index=1 --query="
    CREATE TABLE t_04536 (id UInt32, vec Array(Float32), tags Array(String),
        INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2))
    ENGINE = MergeTree PARTITION BY (id % 4) ORDER BY id SETTINGS index_granularity = 4"
# The nearest base row to [0, 1] is id = 0 (vec = [0, 1]); give it an empty `tags` so `arrayJoin` drops it.
${CLICKHOUSE_CLIENT} --query="INSERT INTO t_04536
    SELECT number, [toFloat32(number), toFloat32(number + 1)], if(number = 0, [], ['tag' || toString(number)])
    FROM numbers(64)"

# `EXPLAIN indexes = 1` lists the `vector_similarity` skip index only when the optimization engages the index.
# `optimize_move_to_prewhere = 0` keeps a WHERE predicate as a `FilterStep`, exercising the filter-chain guard.
# `enable_parallel_replicas = 0`: the optimization is disabled on a distributed plan.
uses_vector_index() {
    ${CLICKHOUSE_CLIENT} --query="EXPLAIN indexes = 1 $1" 2>&1 | grep -c "vector_similarity"
}
tail="ORDER BY cosineDistance(vec, [0.0, 1.0]) LIMIT 1 SETTINGS enable_parallel_replicas = 0, optimize_move_to_prewhere = 0"
control=$(uses_vector_index "SELECT id FROM t_04536 $tail")
aj_select=$(uses_vector_index "SELECT arrayJoin(tags) FROM t_04536 $tail")
aj_where=$(uses_vector_index "SELECT id FROM t_04536 WHERE arrayJoin(tags) = 'tag5' $tail")
if [ "$control" -ge 1 ] && [ "$aj_select" -eq 0 ] && [ "$aj_where" -eq 0 ]; then
    echo "OK"
else
    echo "FAIL control=$control aj_select=$aj_select aj_where=$aj_where"
fi

# Functional regression: the nearest base row (id = 0) expands to zero rows, so the LIMIT 1 must be filled from the
# next-nearest base row (id = 1 -> 'tag1'), not returned short. Without the guard the optimization shortlists only the
# nearest granule and returns the wrong tag (or no row at all).
${CLICKHOUSE_CLIENT} --query="
    SELECT arrayJoin(tags) FROM t_04536 ORDER BY cosineDistance(vec, [0.0, 1.0]) LIMIT 1
    SETTINGS enable_parallel_replicas = 0"

${CLICKHOUSE_CLIENT} --query="DROP TABLE t_04536"

# --- useVectorSearchWithQuantizedCodes (Quantized-coded column) ---
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_04536_q"
${CLICKHOUSE_CLIENT} --allow_experimental_codecs=1 --query="
    CREATE TABLE t_04536_q (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64)), tags Array(String))
    ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query="
    INSERT INTO t_04536_q (id, vec, tags)
    SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)),
        if(number = 0, [], ['tag' || toString(number)]) FROM numbers(4000)"

# The two-stage rewrite adds an inner "quantized shortlist" `LimitStep` to the plan. It is analyzer-only and opt-in via
# `vector_search_use_quantized_codes`.
has_shortlist() {
    ${CLICKHOUSE_CLIENT} --query="EXPLAIN PLAN $1" 2>&1 | grep -ci "quantized shortlist"
}
qtail="ORDER BY cosineDistance(vec, (SELECT vec FROM t_04536_q WHERE id = 123)) ASC LIMIT 5
    SETTINGS vector_search_use_quantized_codes = 1, enable_analyzer = 1, enable_parallel_replicas = 0,
             vector_search_index_fetch_multiplier = 50, optimize_move_to_prewhere = 0,
             query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 1000000"
q_control=$(has_shortlist "SELECT id FROM t_04536_q $qtail")
# arrayJoin in the projection is caught by the rescore-expression guard; arrayJoin in the WHERE is caught by the
# intervening Expression/Filter chain guard.
q_aj_select=$(has_shortlist "SELECT arrayJoin(tags) FROM t_04536_q $qtail")
q_aj_where=$(has_shortlist "SELECT id FROM t_04536_q WHERE arrayJoin(tags) = 'tag5' $qtail")
if [ "$q_control" -ge 1 ] && [ "$q_aj_select" -eq 0 ] && [ "$q_aj_where" -eq 0 ]; then
    echo "OK"
else
    echo "FAIL q_control=$q_control q_aj_select=$q_aj_select q_aj_where=$q_aj_where"
fi

${CLICKHOUSE_CLIENT} --query="DROP TABLE t_04536_q"
