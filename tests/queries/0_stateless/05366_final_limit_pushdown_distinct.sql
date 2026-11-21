-- Regression: LIMIT must not be pushed into the FINAL merge when a preliminary DISTINCT
-- runs *after* the merge. The first `N` merged rows can collapse to fewer than `N` distinct
-- values, so stopping the merge after `N` rows would miss the values that fill the top-N.

DROP TABLE IF EXISTS t_final_distinct;

CREATE TABLE t_final_distinct (key UInt64, val SimpleAggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree
ORDER BY key;

-- Two parts so that FINAL has something to merge.
SYSTEM STOP MERGES t_final_distinct;
INSERT INTO t_final_distinct SELECT number, 1 FROM numbers(100);
INSERT INTO t_final_distinct SELECT number, 2 FROM numbers(100);

-- `intDiv(key, 10)` is monotonic in the sorting key, so the read is done in order, while
-- the first 5 merged rows (keys 0..4) all have the same group 0.
SELECT 'distinct_top5_pushdown_on';
SELECT DISTINCT intDiv(key, 10) AS g
FROM t_final_distinct FINAL
ORDER BY g
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1, optimize_read_in_order = 1;

SELECT 'distinct_top5_pushdown_off';
SELECT DISTINCT intDiv(key, 10) AS g
FROM t_final_distinct FINAL
ORDER BY g
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0, optimize_read_in_order = 1;

-- With the optimization on, no `Description: limit ...` must be attached to the
-- merging transform when a DISTINCT sits between the SortingStep and the FINAL read.
SELECT 'explain_distinct_no_limit_in_merge';
SELECT count()
FROM (
    EXPLAIN PIPELINE
    SELECT DISTINCT intDiv(key, 10) AS g
    FROM t_final_distinct FINAL
    ORDER BY g
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1, optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit%';

-- Sanity check: without DISTINCT the same query pushes the limit into the merge.
SELECT 'explain_no_distinct_limit_in_merge';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT intDiv(key, 10) AS g
    FROM t_final_distinct FINAL
    ORDER BY g
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1, optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit 5%';

DROP TABLE t_final_distinct;
