-- Tags: no-random-settings, no-random-merge-tree-settings

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_lazy_mat_final;

CREATE TABLE t_lazy_mat_final
(
    key UInt64,
    version UInt64,
    category String,
    payload String
)
ENGINE = ReplacingMergeTree(version)
ORDER BY key
SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_lazy_mat_final;

-- Two overlapping parts: version 2 updates keys 0..4999.
INSERT INTO t_lazy_mat_final SELECT
    number, 1,
    if(number % 3 = 0, 'a', if(number % 3 = 1, 'b', 'c')),
    repeat('x', 100)
FROM numbers(10000);

INSERT INTO t_lazy_mat_final SELECT
    number, 2,
    if(number % 3 = 0, 'a', if(number % 3 = 1, 'b', 'c')),
    repeat('y', 100)
FROM numbers(5000);

-- Correctness: ORDER BY non-sorting-key column with LIMIT.
-- payload should be lazily materialized.
SELECT '-- ORDER BY category LIMIT 5';
SELECT key, category, substring(payload, 1, 1) AS p FROM t_lazy_mat_final FINAL
ORDER BY category, key LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 0;

SELECT key, category, substring(payload, 1, 1) AS p FROM t_lazy_mat_final FINAL
ORDER BY category, key LIMIT 5;

-- Plan check: should have LazilyReadFromMergeTree.
SELECT '-- plan has lazy read';
SELECT explain LIKE '%LazilyReadFromMergeTree%' FROM (
    EXPLAIN actions = 0
    SELECT key, category, substring(payload, 1, 1) AS p FROM t_lazy_mat_final FINAL
    ORDER BY category, key LIMIT 5
) WHERE explain LIKE '%LazilyReadFromMergeTree%';

-- Correctness with is_deleted.
DROP TABLE t_lazy_mat_final;

CREATE TABLE t_lazy_mat_final
(
    key UInt64,
    version UInt64,
    is_deleted UInt8,
    category String,
    payload String
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY key
SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_lazy_mat_final;

INSERT INTO t_lazy_mat_final SELECT number, 1, 0, 'a', repeat('x', 100) FROM numbers(1000);
INSERT INTO t_lazy_mat_final SELECT number, 2, if(number < 100, 1, 0), 'a', repeat('y', 100) FROM numbers(500);

SELECT '-- is_deleted with lazy materialization';
SELECT count(), substring(payload, 1, 1) AS p FROM t_lazy_mat_final FINAL
GROUP BY p ORDER BY p LIMIT 10
SETTINGS query_plan_optimize_lazy_materialization = 0;

SELECT count(), substring(payload, 1, 1) AS p FROM t_lazy_mat_final FINAL
GROUP BY p ORDER BY p LIMIT 10;

DROP TABLE t_lazy_mat_final;
