-- Regression test for issue #96452: LazilyReadFromMergeTree optimization
-- was disabled when selecting ALIAS columns with ORDER BY … LIMIT.

SET enable_analyzer = 1, query_plan_optimize_lazy_materialization = true, query_plan_max_limit_for_lazy_materialization = 10;

DROP TABLE IF EXISTS test_lazy_alias SYNC;
CREATE TABLE test_lazy_alias
(
    time       DateTime64(3),
    body       String,
    body_alias String ALIAS if(length(body) > 5, 'long', 'short'),
    severity   LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 100;

INSERT INTO test_lazy_alias
SELECT
    toDateTime64('2020-01-01 00:00:00', 3) - number AS time,
    repeat('x', number % 20) AS body,
    if(number % 2 == 0, 'info', 'medium') AS severity
FROM numbers(10000);

-- 1. Baseline: LazilyReadFromMergeTree appears when selecting a physical column.
SELECT 'physical_column_plan';
SELECT trimLeft(explain) AS s
FROM (EXPLAIN SELECT body FROM test_lazy_alias ORDER BY time DESC LIMIT 10)
WHERE s LIKE 'LazilyRead%';

-- 2. Same optimization must also apply when selecting an ALIAS column.
SELECT 'alias_column_plan';
SELECT trimLeft(explain) AS s
FROM (EXPLAIN SELECT body_alias FROM test_lazy_alias ORDER BY time DESC LIMIT 10)
WHERE s LIKE 'LazilyRead%';

-- 3. The reported regression is the filtered top-K shape `WHERE ... ORDER BY ... LIMIT`.
--    Assert the optimization is applied there too, not only for the unfiltered query,
--    so result correctness alone cannot hide a lost optimization.
SELECT 'alias_filtered_plan';
SELECT trimLeft(explain) AS s
FROM (EXPLAIN SELECT body_alias FROM test_lazy_alias WHERE severity = 'medium' ORDER BY time DESC LIMIT 10)
WHERE s LIKE 'LazilyRead%';

-- 3b. The regression also disabled the top-K optimization for the ALIAS shape.
--     `tryExecuteFunctionsAfterSorting` lifts the ALIAS expression above `Sort`
--     (the expression does not depend on the sort column), turning the plan into
--     `Limit -> Expression -> Sorting`, which made `tryOptimizeTopK` bail out before
--     enabling skip-index / dynamic top-K filtering. The top-K dynamic prewhere filter
--     (`__topKFilter`) must now be present for the ALIAS query exactly as it is for the
--     physical column, so that the same number of rows is scanned (issue #96452).
SELECT 'physical_topk_filter';
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT body FROM test_lazy_alias ORDER BY time DESC LIMIT 10
      SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000)
WHERE explain LIKE '%__topKFilter%';

SELECT 'alias_topk_filter';
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT body_alias FROM test_lazy_alias ORDER BY time DESC LIMIT 10
      SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000)
WHERE explain LIKE '%__topKFilter%';

-- 4. Verify correctness: ALIAS column result must match the expression on source column.
SELECT 'alias_result';
SELECT body_alias
FROM test_lazy_alias
WHERE severity = 'medium'
ORDER BY time DESC
LIMIT 10;

SELECT 'explicit_result';
SELECT if(length(body) > 5, 'long', 'short') AS body_alias
FROM test_lazy_alias
WHERE severity = 'medium'
ORDER BY time DESC
LIMIT 10;

DROP TABLE IF EXISTS test_lazy_alias SYNC;

-- 5. The exact shape of the reproducer in issue #96452: `WHERE` + `ORDER BY ... LIMIT`
--    selecting an `ALIAS` column on a table with a minmax index on the sort column.
--    `liftUpFunctions` splits the expression below `Sort` and leaves the renaming
--    `ExpressionStep` (`time` -> `__table1.time`) separate from the `FilterStep` below
--    it. `tryOptimizeTopK` resolved the sort column through only one of the two steps,
--    so for this shape it could not map the sort column to a storage column and silently
--    dropped both the skip-index top-K read optimization and the dynamic top-K filter:
--    the query read all candidate rows instead of skipping granules beyond the top-K
--    threshold. The queries on the physical column were unaffected (nothing is lifted,
--    so the rename ends up merged into the `FilterStep`).
DROP TABLE IF EXISTS test_lazy_alias_topk SYNC;
CREATE TABLE test_lazy_alias_topk
(
    time       DateTime64(3),
    body       String,
    body_alias String ALIAS if(length(body) > 5, 'long', 'short'),
    severity   LowCardinality(String),
    INDEX time_minmax(time) TYPE minmax GRANULARITY 1,
    INDEX severity_set(severity) TYPE set(10) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 100;

INSERT INTO test_lazy_alias_topk
SELECT
    toDateTime64('2020-01-01 00:00:00', 3) - number AS time,
    repeat('x', number % 20) AS body,
    if(number % 2 == 0, 'info', 'medium') AS severity
FROM numbers(10000);

-- 5a. The dynamic top-K prewhere filter must be present for the filtered ALIAS query
--     exactly as it is for the physical column.
SELECT 'alias_filtered_topk_filter';
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT body_alias FROM test_lazy_alias_topk WHERE severity = 'medium' ORDER BY time DESC LIMIT 10
      SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000)
WHERE explain LIKE '%__topKFilter%';

SELECT 'physical_filtered_topk_filter';
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT body FROM test_lazy_alias_topk WHERE severity = 'medium' ORDER BY time DESC LIMIT 10
      SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000)
WHERE explain LIKE '%__topKFilter%';

-- 5b. The skip-index top-K path must skip granules beyond the top-K threshold during
--     the read. The top rows by `time` sit in the first granules, so once the threshold
--     is established the minmax index rejects the remaining granules: the query must
--     read only a fraction of the table instead of all 10000 rows. `max_block_size` is
--     small so the threshold is published before the whole (small) table is read, and
--     `max_threads = 1` keeps the amount of read-ahead bounded. `max_rows_to_read` must be
--     reset: the CI test profile sets it to 20M, and a non-zero value together with the
--     default `read_overflow_mode = 'throw'` disables the whole on-data-read skip-index
--     path (see `ReadFromMergeTree::supportsSkipIndexesOnDataRead`).
SELECT 'alias_filtered_skip_index_execution';
SELECT body_alias FROM test_lazy_alias_topk WHERE severity = 'medium' ORDER BY time DESC LIMIT 3
SETTINGS use_skip_indexes = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1,
         use_top_k_dynamic_filtering = 0, query_plan_max_limit_for_top_k_optimization = 1000,
         max_threads = 1, max_block_size = 128, enable_parallel_replicas = 0, max_rows_to_read = 0,
         merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
         log_comment = '04076_alias_filtered_skip_index';

SELECT 'alias_filtered_skip_index_rows_read';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows < 5000
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04076_alias_filtered_skip_index'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE IF EXISTS test_lazy_alias_topk SYNC;
