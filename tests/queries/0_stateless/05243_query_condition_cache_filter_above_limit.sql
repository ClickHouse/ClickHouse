-- Tests that the query condition cache key of a read is attached only to the `FilterStep` right above
-- it, and not to an outer `FilterStep` separated from the read by a `LIMIT`. The outer filter's
-- condition is not part of the key, so granules it empties must not be recorded as not matching the
-- read's filter. Two plans make the outer filter the first `FilterStep` above the read:
-- - an explicit PREWHERE holds the whole inner condition, so no `FilterStep` sits below the `LIMIT`;
-- - the key gets attached again after join runtime filters were added, when `optimizePrewhere` has
--   already moved the whole inner `WHERE` into PREWHERE.
-- In granules where every `a = 1` row has `b = 3`, the outer `WHERE b = 2` returns nothing, and a
-- later read of `a = 1` would skip those granules.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_query_condition_cache = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET query_plan_max_step_description_length = 1000;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_prewhere;
DROP TABLE IF EXISTS dim;

-- `a = 1` holds for a tenth of the rows of every granule, `b = 2` only in the first half of the table.
CREATE TABLE tab (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY c
SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
INSERT INTO tab SELECT number % 10, if(number < 5000, 2, 3), number FROM numbers(10000);

CREATE TABLE dim (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO dim SELECT number FROM numbers(10);

-- A table of its own, so that without the fix the two cases don't share cache entries and each fails on its own.
CREATE TABLE tab_prewhere AS tab;
INSERT INTO tab_prewhere SELECT * FROM tab;

-- Explicit PREWHERE. Small blocks, so that granules where every `a = 1` row has `b = 3` reach the outer filter
-- as chunks of their own. Print both counts rather than a boolean, so a reference diff shows which way it broke.
SELECT count() FROM (SELECT a, b FROM tab_prewhere PREWHERE a = 1 LIMIT 100000) WHERE b = 2 SETTINGS max_block_size = 100;
SELECT count() FROM (SELECT a, b FROM tab_prewhere PREWHERE a = 1 LIMIT 100000) WHERE b = 3 SETTINGS use_query_condition_cache = 1;
SELECT count() FROM (SELECT a, b FROM tab_prewhere PREWHERE a = 1 LIMIT 100000) WHERE b = 3 SETTINGS use_query_condition_cache = 0;

-- Join runtime filters. Pins the shape the row counts below depend on: the runtime filter exists, and the outer
-- filter sits on a `LIMIT` over the read whose PREWHERE holds the whole inner `WHERE`.
SELECT
    countIf(explain LIKE 'BuildRuntimeFilter (%') > 0,
    match(arrayStringConcat(groupArray(explain), '\n'),
        'Filter column: equals\\(__table\\d+\\.b, 2_UInt8\\)[^\\n]*\\n(Expression[^\\n]*\\n)*Limit \\(preliminary LIMIT\\)\\n(Expression[^\\n]*\\n)*ReadFromMergeTree \\([^\\n]*\\.tab\\)\\nPrewhere filter column: equals\\(__table\\d+\\.a, 1_UInt8\\)')
FROM
(
    SELECT trimLeft(explain) AS explain
    FROM (EXPLAIN actions = 1, pretty = 0
        SELECT count() FROM dim AS d JOIN (SELECT * FROM (SELECT a, b FROM tab WHERE a = 1 LIMIT 100000) WHERE b = 2) AS s ON d.x = s.a
        SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, query_plan_join_swap_table = 0)
    WHERE match(trimLeft(explain), '^([A-Z][A-Za-z]+( \\(|$)|Filter column:|Prewhere filter column:)')
);

SELECT count() FROM dim AS d JOIN (SELECT * FROM (SELECT a, b FROM tab WHERE a = 1 LIMIT 100000) WHERE b = 2) AS s ON d.x = s.a
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, query_plan_join_swap_table = 0, max_block_size = 100;

SELECT count() FROM dim AS d JOIN (SELECT * FROM (SELECT a, b FROM tab WHERE a = 1 LIMIT 100000) WHERE b = 3) AS s ON d.x = s.a
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, query_plan_join_swap_table = 0, use_query_condition_cache = 1;
SELECT count() FROM dim AS d JOIN (SELECT * FROM (SELECT a, b FROM tab WHERE a = 1 LIMIT 100000) WHERE b = 3) AS s ON d.x = s.a
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, query_plan_join_swap_table = 0, use_query_condition_cache = 0;

DROP TABLE dim;
DROP TABLE tab_prewhere;
DROP TABLE tab;
