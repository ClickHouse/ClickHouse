-- Regression: LIMIT must not be pushed into the FINAL merge when a row-reducing
-- step (ARRAY JOIN that can drop rows, or preliminary DISTINCT) runs *after* the
-- merge. Otherwise the merge can stop after the first N keys even though some of
-- those rows are removed by the later step, so keys that should fill the top-N
-- are never read.

DROP TABLE IF EXISTS t_final_arr;

CREATE TABLE t_final_arr
(
    key UInt64,
    arr Array(UInt64),
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY key;

-- Two parts per key so that FINAL has something to merge.
-- The smallest keys (0..4) have an empty array, so ARRAY JOIN removes them;
-- the correct top-5 by key after ARRAY JOIN must therefore start at key = 5.
INSERT INTO t_final_arr
    SELECT number, if(number < 5, [], [toUInt64(1)]), sumState(toUInt64(1))
    FROM numbers(100) GROUP BY number;
INSERT INTO t_final_arr
    SELECT number, if(number < 5, [], [toUInt64(1)]), sumState(toUInt64(2))
    FROM numbers(100) GROUP BY number;

SELECT 'array_join_top5_pushdown_on';
SELECT key, finalizeAggregation(val)
FROM t_final_arr FINAL
ARRAY JOIN arr
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'array_join_top5_pushdown_off';
SELECT key, finalizeAggregation(val)
FROM t_final_arr FINAL
ARRAY JOIN arr
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- With the optimization ON, no `Description: limit ...` must be attached to the
-- merging transform when an ARRAY JOIN sits between the SortingStep and the
-- FINAL read.
SELECT 'explain_array_join_no_limit_in_merge';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, finalizeAggregation(val)
    FROM t_final_arr FINAL
    ARRAY JOIN arr
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1
)
WHERE explain LIKE '%Description: limit%';

DROP TABLE t_final_arr;
