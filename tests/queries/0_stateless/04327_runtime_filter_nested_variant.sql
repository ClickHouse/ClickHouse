-- Runtime filter on a join key that contains a Variant/Dynamic nested inside a compound
-- type (Tuple/Array/Map). A Variant can be NULL without a Nullable wrapper, but the nested
-- case was not detected, so the single-element "equals" runtime filter path was chosen and
-- produced Nullable(UInt8), tripping the __applyFilter return-type assertion.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET allow_suspicious_types_in_order_by = 1;

-- Tuple(Variant, ...), exactly 1 distinct value on the right (the "equals" path).
SELECT *
FROM
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', 'true\nfalse')) AS t1
    JOIN
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', 'true')) AS t2
    USING (k)
ORDER BY k;

-- Tuple(Variant, ...), several distinct values on the right (the set-lookup path).
SELECT *
FROM
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', 'true\nfalse\nstr')) AS t1
    JOIN
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', 'true\nstr')) AS t2
    USING (k)
ORDER BY k;

-- Array(Variant) as the join key.
SELECT *
FROM
    (SELECT [v] AS k FROM format(TSV, 'v Variant(String, Bool)', 'true\nfalse')) AS t1
    JOIN
    (SELECT [v] AS k FROM format(TSV, 'v Variant(String, Bool)', 'true')) AS t2
    USING (k)
ORDER BY k;

-- Map(String, Variant) as the join key.
SELECT *
FROM
    (SELECT map('a', v) AS k FROM format(TSV, 'v Variant(String, Bool)', 'true\nfalse')) AS t1
    JOIN
    (SELECT map('a', v) AS k FROM format(TSV, 'v Variant(String, Bool)', 'true')) AS t2
    USING (k)
ORDER BY k;

-- Dynamic behaves like Variant here (NULL without a Nullable wrapper), so the same nested
-- cases must be covered to guard the hasVariantOrDynamic() contract for Dynamic.
SET allow_dynamic_type_in_join_keys = 1;

-- Tuple(Dynamic, ...), exactly 1 distinct value on the right (the "equals" path).
SELECT *
FROM
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Dynamic', 'true\nfalse')) AS t1
    JOIN
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Dynamic', 'true')) AS t2
    USING (k)
ORDER BY k;

-- Tuple(Dynamic, ...), several distinct values on the right (the set-lookup path).
SELECT *
FROM
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Dynamic', 'true\nfalse\nstr')) AS t1
    JOIN
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Dynamic', 'true\nstr')) AS t2
    USING (k)
ORDER BY k;

-- Array(Dynamic) as the join key.
SELECT *
FROM
    (SELECT [v] AS k FROM format(TSV, 'v Dynamic', 'true\nfalse')) AS t1
    JOIN
    (SELECT [v] AS k FROM format(TSV, 'v Dynamic', 'true')) AS t2
    USING (k)
ORDER BY k;

-- Map(String, Dynamic) as the join key.
SELECT *
FROM
    (SELECT map('a', v) AS k FROM format(TSV, 'v Dynamic', 'true\nfalse')) AS t1
    JOIN
    (SELECT map('a', v) AS k FROM format(TSV, 'v Dynamic', 'true')) AS t2
    USING (k)
ORDER BY k;

-- LowCardinality(Nullable(...)) nested in a Tuple: also covered by hasTypeThatCanContainNulls().
-- One distinct value on the right (the "equals" path).
SELECT *
FROM
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v LowCardinality(Nullable(String))', 'a\nb')) AS t1
    JOIN
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v LowCardinality(Nullable(String))', 'a')) AS t2
    USING (k)
ORDER BY k;

-- Several distinct values on the right (the set-lookup path).
SELECT *
FROM
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v LowCardinality(Nullable(String))', 'a\nb\nc')) AS t1
    JOIN
    (SELECT tuple(v, 1) AS k FROM format(TSV, 'v LowCardinality(Nullable(String))', 'a\nc')) AS t2
    USING (k)
ORDER BY k;

-- LEFT ANTI JOIN: the runtime filter must not drop rows that the join keeps. A nested NULL key
-- (Variant/Dynamic NULL, or nested Nullable/LowCardinality(Nullable)) compares null-safely in the
-- join, so a nested-NULL key present on both sides DOES match and the anti-join drops it - exactly
-- what the exclusion runtime filter (transform_null_in) does. Each case asserts the rows with
-- runtime filters off equal the rows with them on. 1 = consistent.
-- '\\N' is a TSV NULL marker; the literal needs the doubled backslash so the SQL lexer passes '\N'.

-- Tuple(Variant): one distinct value on the right (the "equals" path), NULL on both sides.
SELECT
    (SELECT arraySort(groupArray(toString(k))) FROM (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N\ntrue')) AS t1 LEFT ANTI JOIN (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 0)
  = (SELECT arraySort(groupArray(toString(k))) FROM (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N\ntrue')) AS t1 LEFT ANTI JOIN (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 1);

-- Tuple(Variant): several distinct values on the right (the set path with negative lookup), NULL on both sides.
SELECT
    (SELECT arraySort(groupArray(toString(k))) FROM (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N\ntrue\nfalse\nstr')) AS t1 LEFT ANTI JOIN (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N\ntrue')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 0)
  = (SELECT arraySort(groupArray(toString(k))) FROM (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N\ntrue\nfalse\nstr')) AS t1 LEFT ANTI JOIN (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N\ntrue')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 1);

-- Array(Variant), NULL on both sides.
SELECT
    (SELECT arraySort(groupArray(toString(k))) FROM (SELECT [v] AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N\ntrue\nstr')) AS t1 LEFT ANTI JOIN (SELECT [v] AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 0)
  = (SELECT arraySort(groupArray(toString(k))) FROM (SELECT [v] AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N\ntrue\nstr')) AS t1 LEFT ANTI JOIN (SELECT [v] AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 1);

-- Map(String, Variant), NULL on both sides.
SELECT
    (SELECT arraySort(groupArray(toString(k))) FROM (SELECT map('a', v) AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N\ntrue\nstr')) AS t1 LEFT ANTI JOIN (SELECT map('a', v) AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 0)
  = (SELECT arraySort(groupArray(toString(k))) FROM (SELECT map('a', v) AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N\ntrue\nstr')) AS t1 LEFT ANTI JOIN (SELECT map('a', v) AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 1);

SET allow_dynamic_type_in_join_keys = 1;

-- Tuple(Dynamic): set path with negative lookup, NULL on both sides.
SELECT
    (SELECT arraySort(groupArray(toString(k))) FROM (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Dynamic', '\\N\ntrue\nfalse\nstr')) AS t1 LEFT ANTI JOIN (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Dynamic', '\\N\ntrue')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 0)
  = (SELECT arraySort(groupArray(toString(k))) FROM (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Dynamic', '\\N\ntrue\nfalse\nstr')) AS t1 LEFT ANTI JOIN (SELECT tuple(v, 1) AS k FROM format(TSV, 'v Dynamic', '\\N\ntrue')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 1);

-- Tuple(LowCardinality(Nullable)): set path with negative lookup, NULL on both sides.
SELECT
    (SELECT arraySort(groupArray(toString(k))) FROM (SELECT tuple(v, 1) AS k FROM format(TSV, 'v LowCardinality(Nullable(String))', '\\N\na\nb\nc')) AS t1 LEFT ANTI JOIN (SELECT tuple(v, 1) AS k FROM format(TSV, 'v LowCardinality(Nullable(String))', '\\N\na')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 0)
  = (SELECT arraySort(groupArray(toString(k))) FROM (SELECT tuple(v, 1) AS k FROM format(TSV, 'v LowCardinality(Nullable(String))', '\\N\na\nb\nc')) AS t1 LEFT ANTI JOIN (SELECT tuple(v, 1) AS k FROM format(TSV, 'v LowCardinality(Nullable(String))', '\\N\na')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 1);

-- Map with the null-capable type in the KEY position. hasTypeThatCanContainNulls() recurses into
-- both the Map key and value types, so a Variant/Dynamic Map key must be covered too (the value-side
-- cases above only exercise the value branch). Map(Variant, ...) / Map(Dynamic, ...) reach the same
-- runtime-filter path, and the single-distinct-value "equals" path is what tripped the original
-- __applyFilter assertion when the nested Variant/Dynamic was not detected.

-- Map(Variant, String) key, exactly 1 distinct value on the right (the "equals" path).
SELECT *
FROM
    (SELECT map(v, 'x') AS k FROM format(TSV, 'v Variant(String, Bool)', 'true\nfalse')) AS t1
    JOIN
    (SELECT map(v, 'x') AS k FROM format(TSV, 'v Variant(String, Bool)', 'true')) AS t2
    USING (k)
ORDER BY k;

-- Map(Dynamic, String) key, exactly 1 distinct value on the right (the "equals" path).
SELECT *
FROM
    (SELECT map(v, 'x') AS k FROM format(TSV, 'v Dynamic', 'true\nfalse')) AS t1
    JOIN
    (SELECT map(v, 'x') AS k FROM format(TSV, 'v Dynamic', 'true')) AS t2
    USING (k)
ORDER BY k;

-- Map(Variant, String) key LEFT ANTI JOIN, runtime filters off == on, NULL on both sides.
SELECT
    (SELECT arraySort(groupArray(toString(k))) FROM (SELECT map(v, 'x') AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N\ntrue')) AS t1 LEFT ANTI JOIN (SELECT map(v, 'x') AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 0)
  = (SELECT arraySort(groupArray(toString(k))) FROM (SELECT map(v, 'x') AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N\ntrue')) AS t1 LEFT ANTI JOIN (SELECT map(v, 'x') AS k FROM format(TSV, 'v Variant(String, Bool)', '\\N')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 1);

-- Map(Dynamic, String) key LEFT ANTI JOIN, runtime filters off == on, NULL on both sides.
SELECT
    (SELECT arraySort(groupArray(toString(k))) FROM (SELECT map(v, 'x') AS k FROM format(TSV, 'v Dynamic', '\\N\ntrue')) AS t1 LEFT ANTI JOIN (SELECT map(v, 'x') AS k FROM format(TSV, 'v Dynamic', '\\N')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 0)
  = (SELECT arraySort(groupArray(toString(k))) FROM (SELECT map(v, 'x') AS k FROM format(TSV, 'v Dynamic', '\\N\ntrue')) AS t1 LEFT ANTI JOIN (SELECT map(v, 'x') AS k FROM format(TSV, 'v Dynamic', '\\N')) AS t2 USING (k) SETTINGS enable_join_runtime_filters = 1);

-- Build side coverage: the runtime-filter exact-values Set is built incrementally, so each build block
-- after the first goes through Set::appendSetElements -> ColumnVariant/ColumnDynamic insertRangeFrom, and
-- each per-thread filter is combined via ExactContainsRuntimeFilter::merge. numbers() with a small
-- max_block_size forces many tiny build blocks (so the insertRangeFrom path runs); the merge path needs a
-- multi-stream build source (numbers_mt) because numbers() is single-stream. query_plan_join_swap_table
-- = false keeps the right table the build side. count() is join-order/block-size invariant.
-- Each case also asserts the plan/pipeline so it fails (not silently passes) if the planner stops
-- building the runtime filter for this shape: EXPLAIN actions = 1 must contain a BuildRuntimeFilter
-- action, and the numbers_mt merge case must show multiple BuildRuntimeFilterTransform instances.

-- Variant key, many tiny build blocks (repeated appendSetElements insertRangeFrom).
SELECT count() FROM
    (SELECT multiIf(number % 2 = 0, number::Variant(UInt64, String), toString(number)::Variant(UInt64, String)) AS k FROM numbers(8)) AS t1
    JOIN
    (SELECT multiIf(number % 2 = 0, number::Variant(UInt64, String), toString(number)::Variant(UInt64, String)) AS k FROM numbers(40)) AS t2
    USING (k)
SETTINGS max_block_size = 1, max_threads = 4, query_plan_join_swap_table = false;
SELECT count() > 0 FROM (EXPLAIN actions = 1
SELECT count() FROM
    (SELECT multiIf(number % 2 = 0, number::Variant(UInt64, String), toString(number)::Variant(UInt64, String)) AS k FROM numbers(8)) AS t1
    JOIN
    (SELECT multiIf(number % 2 = 0, number::Variant(UInt64, String), toString(number)::Variant(UInt64, String)) AS k FROM numbers(40)) AS t2
    USING (k)
SETTINGS max_block_size = 1, max_threads = 4, query_plan_join_swap_table = false, enable_join_runtime_filters = 1
) WHERE explain ILIKE '%BuildRuntimeFilter%';

-- Dynamic key, consecutive small blocks see the alternatives in different first-seen order, so
-- insertRangeFrom must reconcile diverging local discriminators.
SELECT count() FROM
    (SELECT multiIf(number % 4 IN (0, 3), number::Int64::Dynamic, toString(number)::Dynamic) AS k FROM numbers(8)) AS t1
    JOIN
    (SELECT multiIf(number % 4 IN (0, 3), number::Int64::Dynamic, toString(number)::Dynamic) AS k FROM numbers(40)) AS t2
    USING (k)
SETTINGS max_block_size = 2, max_threads = 1, query_plan_join_swap_table = false;
SELECT count() > 0 FROM (EXPLAIN actions = 1
SELECT count() FROM
    (SELECT multiIf(number % 4 IN (0, 3), number::Int64::Dynamic, toString(number)::Dynamic) AS k FROM numbers(8)) AS t1
    JOIN
    (SELECT multiIf(number % 4 IN (0, 3), number::Int64::Dynamic, toString(number)::Dynamic) AS k FROM numbers(40)) AS t2
    USING (k)
SETTINGS max_block_size = 2, max_threads = 1, query_plan_join_swap_table = false, enable_join_runtime_filters = 1
) WHERE explain ILIKE '%BuildRuntimeFilter%';

-- Dynamic key, parallel hash so several per-thread filters are built and merged (ExactContainsRuntimeFilter::merge).
-- numbers_mt (not numbers) is required: numbers() is forced to one stream, so BuildRuntimeFilterStep would
-- capture a single build stream (filters_to_merge = 0) and merge() would never run. numbers_mt fans the build
-- side across max_threads streams, giving several BuildRuntimeFilterTransform instances whose filters merge.
SELECT count() FROM
    (SELECT multiIf(number % 4 = 0, number::Int64::Dynamic, number % 4 = 1, toString(number)::Dynamic, number % 4 = 2, number::Float64::Dynamic, (number % 2)::UInt8::Dynamic) AS k FROM numbers(200)) AS t1
    JOIN
    (SELECT multiIf(number % 4 = 0, number::Int64::Dynamic, number % 4 = 1, toString(number)::Dynamic, number % 4 = 2, number::Float64::Dynamic, (number % 2)::UInt8::Dynamic) AS k FROM numbers_mt(2000)) AS t2
    USING (k)
SETTINGS max_block_size = 37, max_threads = 8, join_algorithm = 'parallel_hash', query_plan_join_swap_table = false;
SELECT count() > 0 FROM (EXPLAIN actions = 1
SELECT count() FROM
    (SELECT multiIf(number % 4 = 0, number::Int64::Dynamic, number % 4 = 1, toString(number)::Dynamic, number % 4 = 2, number::Float64::Dynamic, (number % 2)::UInt8::Dynamic) AS k FROM numbers(200)) AS t1
    JOIN
    (SELECT multiIf(number % 4 = 0, number::Int64::Dynamic, number % 4 = 1, toString(number)::Dynamic, number % 4 = 2, number::Float64::Dynamic, (number % 2)::UInt8::Dynamic) AS k FROM numbers_mt(2000)) AS t2
    USING (k)
SETTINGS max_block_size = 37, max_threads = 8, join_algorithm = 'parallel_hash', query_plan_join_swap_table = false, enable_join_runtime_filters = 1
) WHERE explain ILIKE '%BuildRuntimeFilter%';
-- The pipeline must show more than one BuildRuntimeFilterTransform (the numbers_mt build side is multi-stream),
-- otherwise filters_to_merge = 0 and ExactContainsRuntimeFilter::merge is never exercised.
SELECT count() > 0 FROM (EXPLAIN PIPELINE
SELECT count() FROM
    (SELECT multiIf(number % 4 = 0, number::Int64::Dynamic, number % 4 = 1, toString(number)::Dynamic, number % 4 = 2, number::Float64::Dynamic, (number % 2)::UInt8::Dynamic) AS k FROM numbers(200)) AS t1
    JOIN
    (SELECT multiIf(number % 4 = 0, number::Int64::Dynamic, number % 4 = 1, toString(number)::Dynamic, number % 4 = 2, number::Float64::Dynamic, (number % 2)::UInt8::Dynamic) AS k FROM numbers_mt(2000)) AS t2
    USING (k)
SETTINGS max_block_size = 37, max_threads = 8, join_algorithm = 'parallel_hash', query_plan_join_swap_table = false, enable_join_runtime_filters = 1
) WHERE explain LIKE '%BuildRuntimeFilterTransform × %';

-- Tuple(Dynamic, ...) key, many tiny build blocks with diverging local order.
SELECT count() FROM
    (SELECT tuple(multiIf(number % 4 IN (0, 3), number::Int64::Dynamic, toString(number)::Dynamic), 1) AS k FROM numbers(8)) AS t1
    JOIN
    (SELECT tuple(multiIf(number % 4 IN (0, 3), number::Int64::Dynamic, toString(number)::Dynamic), 1) AS k FROM numbers(40)) AS t2
    USING (k)
SETTINGS max_block_size = 1, max_threads = 2, query_plan_join_swap_table = false;
SELECT count() > 0 FROM (EXPLAIN actions = 1
SELECT count() FROM
    (SELECT tuple(multiIf(number % 4 IN (0, 3), number::Int64::Dynamic, toString(number)::Dynamic), 1) AS k FROM numbers(8)) AS t1
    JOIN
    (SELECT tuple(multiIf(number % 4 IN (0, 3), number::Int64::Dynamic, toString(number)::Dynamic), 1) AS k FROM numbers(40)) AS t2
    USING (k)
SETTINGS max_block_size = 1, max_threads = 2, query_plan_join_swap_table = false, enable_join_runtime_filters = 1
) WHERE explain ILIKE '%BuildRuntimeFilter%';
