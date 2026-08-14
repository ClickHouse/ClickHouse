-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- Lazy FINAL (InputSelector) must survive join-order estimation, which memoizes
-- an index-analysis result on the reading step: the guard in optimizeLazyFinal
-- must only skip for projection reads, not for any non-null analysis result.
-- Regression test for PR fixing issue #110281.

SET enable_analyzer = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS rmt_04517;
DROP TABLE IF EXISTS dim_04517;

CREATE TABLE rmt_04517 (id UInt64, version UInt64, status String)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS refresh_statistics_interval = 0, index_granularity = 256;

SYSTEM STOP MERGES rmt_04517;

INSERT INTO rmt_04517 SELECT number, 1, if(number < 100, 'target', 'other') FROM numbers(10000);
INSERT INTO rmt_04517 SELECT number, 2, if(number < 100, 'target', 'other') FROM numbers(10000);

CREATE TABLE dim_04517 (id UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS refresh_statistics_interval = 0;

INSERT INTO dim_04517 SELECT number FROM numbers(1000);

-- Control: single-table FINAL gets lazy FINAL.
SELECT count() > 0
FROM (
    EXPLAIN
    SELECT count() FROM rmt_04517 FINAL WHERE status = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000
)
WHERE explain ILIKE '%InputSelector%';

-- FINAL under a JOIN with statistics enabled must keep lazy FINAL.
SELECT count() > 0
FROM (
    EXPLAIN
    SELECT count()
    FROM rmt_04517 AS f FINAL
    INNER JOIN dim_04517 AS d ON f.id = d.id
    WHERE f.status = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000,
             use_statistics = 1, use_statistics_cache = 0, collect_hash_table_stats_during_joins = 0
)
WHERE explain ILIKE '%InputSelector%';

-- Same without statistics: the index-based fallback also memoizes analysis
-- and must not disable lazy FINAL either (pre-existing variant of the bug).
SELECT count() > 0
FROM (
    EXPLAIN
    SELECT count()
    FROM rmt_04517 AS f FINAL
    INNER JOIN dim_04517 AS d ON f.id = d.id
    WHERE f.status = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000,
             use_statistics = 0, collect_hash_table_stats_during_joins = 0
)
WHERE explain ILIKE '%InputSelector%';

-- Correctness: FINAL semantics under the join are unchanged by lazy FINAL.
SELECT count()
FROM rmt_04517 AS f FINAL
INNER JOIN dim_04517 AS d ON f.id = d.id
WHERE f.status = 'target'
SETTINGS query_plan_optimize_lazy_final = 0;

SELECT count()
FROM rmt_04517 AS f FINAL
INNER JOIN dim_04517 AS d ON f.id = d.id
WHERE f.status = 'target'
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000;

DROP TABLE rmt_04517;
DROP TABLE dim_04517;
