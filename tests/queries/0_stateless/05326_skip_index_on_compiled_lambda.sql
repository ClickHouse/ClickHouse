-- Tags: no-fasttest
-- Index analysis matches a lambda by a name built from its body. A JIT-compiled lambda body used to be
-- named after the compiled expression, so a skip index over the same lambda expression was not used.

-- The reference has the legacy `EXPLAIN indexes = 1` layout; the default format is `pretty`.
SET explain_query_plan_default = 'legacy';
-- `auto_statistics_types` is randomized, and part pruning by statistics would change the `EXPLAIN` output.
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_05326;

-- Granule counts in the reference depend on the granularity, which is randomized otherwise.
CREATE TABLE t_05326 (id UInt64, arr Array(UInt64), INDEX idx arrayExists(x -> x * 3 + 1 > 5000, arr) TYPE set(2) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi';
INSERT INTO t_05326 SELECT number, if(number < 1000, [number * 10], [1, 2]) FROM numbers(100000);

-- A query that does not use `idx` fails with `INDEX_NOT_USED`.
SET force_data_skipping_indices = 'idx';

-- `compile_expressions` and `min_count_to_compile_expression` are randomized: the first query never compiles
-- the lambda, the second one compiles it on the first build of the expression.
SELECT count() FROM t_05326 WHERE arrayExists(x -> x * 3 + 1 > 5000, arr) SETTINGS compile_expressions = 0;
SELECT count() FROM t_05326 WHERE arrayExists(x -> x * 3 + 1 > 5000, arr) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

-- Granules selected by `idx`. The total is left out: a wide part has one more (final) mark than a compact
-- one, and the part type is randomized.
SELECT replaceRegexpOne(arrayFirst(l -> startsWith(l, 'Granules:'), arraySlice(lines, indexOf(lines, 'Name: idx'))), '/\\d+$', '') FROM (
    SELECT groupArray(trim(explain)) AS lines FROM (
        EXPLAIN indexes = 1 SELECT count() FROM t_05326 WHERE arrayExists(x -> x * 3 + 1 > 5000, arr)
        SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0));

DROP TABLE t_05326;
