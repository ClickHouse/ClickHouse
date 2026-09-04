-- A filter fused into the ARRAY JOIN step (`query_plan_fuse_filter_into_array_join`) runs its own
-- ExpressionActions in element space. With `enable_adaptive_short_circuit_lazy_execution` that instance
-- is adaptive, so it is stateful and every stream gets its own copy: the results must stay identical
-- to the non-adaptive and the unfused plans, with several streams running concurrently.
SET enable_analyzer = 1;
-- fusion is skipped for serialized plans, pin it so the fused plan is actually used
SET serialize_query_plan = 0;
SET short_circuit_function_evaluation = 'force_enable';
SET max_threads = 8;

DROP TABLE IF EXISTS t_fused_adaptive;
CREATE TABLE t_fused_adaptive (id UInt64, arr Array(UInt64), payload String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_fused_adaptive SELECT number, arrayMap(x -> x % 4, range(number % 7)), repeat('P', 64) FROM numbers(200000);

-- An element predicate whose second argument throws for the elements the first one rejects:
-- the fused element filter must keep short-circuiting it under the adaptive mode as well.
SELECT count(), sum(elem) FROM (SELECT elem FROM t_fused_adaptive ARRAY JOIN arr AS elem WHERE elem != 0 AND intDiv(100, elem) > 20)
SETTINGS query_plan_fuse_filter_into_array_join = 1, enable_adaptive_short_circuit_lazy_execution = 0;

SELECT count(), sum(elem) FROM (SELECT elem FROM t_fused_adaptive ARRAY JOIN arr AS elem WHERE elem != 0 AND intDiv(100, elem) > 20)
SETTINGS query_plan_fuse_filter_into_array_join = 1, enable_adaptive_short_circuit_lazy_execution = 1;

SELECT count(), sum(elem) FROM (SELECT elem FROM t_fused_adaptive ARRAY JOIN arr AS elem WHERE elem != 0 AND intDiv(100, elem) > 20)
SETTINGS query_plan_fuse_filter_into_array_join = 0, enable_adaptive_short_circuit_lazy_execution = 1;

-- An expensive branch behind a cheap one: the adaptive heuristic switches the branch between lazy and
-- eager while the query runs, and the fused filter must return the same rows in either state.
SELECT count(), sum(cityHash64(elem, payload)) FROM (SELECT elem, payload FROM t_fused_adaptive ARRAY JOIN arr AS elem WHERE elem > 1 AND cityHash64(toString(elem)) % 2 = 0)
SETTINGS query_plan_fuse_filter_into_array_join = 1, enable_adaptive_short_circuit_lazy_execution = 0;

SELECT count(), sum(cityHash64(elem, payload)) FROM (SELECT elem, payload FROM t_fused_adaptive ARRAY JOIN arr AS elem WHERE elem > 1 AND cityHash64(toString(elem)) % 2 = 0)
SETTINGS query_plan_fuse_filter_into_array_join = 1, enable_adaptive_short_circuit_lazy_execution = 1;

DROP TABLE t_fused_adaptive;
