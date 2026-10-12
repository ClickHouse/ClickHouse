-- `-0.` and `0.` are equal, but they are different constants: `1 / -0.` is `-inf`.
-- The identity of a constant (the key of the cache of compiled expressions, the deduplication
-- of constants in `ActionsDAG`) must tell them apart, even though hash table keys do not.

-- The function compiled for the first expression must not be used for the second one.
SET compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT groupArray(1 / k) FROM (SELECT if(number % 2 = 0, 0., -0.) AS k FROM numbers(4));
SELECT groupArray(1 / k) FROM (SELECT if(number % 2 = 0, -0., 0.) AS k FROM numbers(4));

-- The two constants must not be merged when a filter is merged with its child expression.
SELECT count() FROM (SELECT if(number % 2 = 0, -0., 0.) AS k FROM numbers(4)) WHERE 1 / k > 0 SETTINGS compile_expressions = 0;
SELECT count() FROM (SELECT if(number % 2 = 0, -0., 0.) AS k FROM numbers(4)) WHERE 1 / k > 0 SETTINGS compile_expressions = 0, query_plan_merge_expressions = 0;

-- On the other hand, the two zeros are one value in the set semantics, also inside `Nullable` and `LowCardinality(Nullable)`.
SELECT uniq(x, 1), uniqCombined(x, 1), uniqExact(x, 1) FROM (SELECT arrayJoin([-0., 0.])::Nullable(Float64) AS x);
SELECT groupUniqArray(x), topK(x), groupArrayDistinct(x) FROM (SELECT arrayJoin([-0., 0.])::LowCardinality(Nullable(Float64)) AS x) SETTINGS allow_suspicious_low_cardinality_types = 1;
