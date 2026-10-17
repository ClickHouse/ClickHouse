-- Regression test: FINAL LIMIT pushdown must be disabled when the SortingStep
-- is marked `always_read_till_end` (set by `exact_rows_before_limit`).
--
-- With `exact_rows_before_limit = 1` the contract is that the whole input is read
-- so that `rows_before_limit_at_least` reflects the full post-FINAL row count.
-- If we pushed the LIMIT into the FINAL merge it would terminate after N groups
-- and report roughly N rows instead of the full count.

DROP TABLE IF EXISTS t_exact_rows;

CREATE TABLE t_exact_rows
(
    key UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY key;

-- Two parts so that FINAL actually merges.
INSERT INTO t_exact_rows SELECT number, sumState(toUInt64(1)) FROM numbers(100) GROUP BY number;
INSERT INTO t_exact_rows SELECT number, sumState(toUInt64(1)) FROM numbers(100) GROUP BY number;

-- Sanity: pushdown is visible when exact_rows_before_limit is off (positive control).
SELECT 'pushdown_on_exact_off';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, finalizeAggregation(val)
    FROM t_exact_rows FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1,
             exact_rows_before_limit = 0
)
WHERE explain LIKE '%Description: limit 5%';

-- With exact_rows_before_limit = 1 the pushdown must be suppressed.
SELECT 'pushdown_off_exact_on';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, finalizeAggregation(val)
    FROM t_exact_rows FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1,
             exact_rows_before_limit = 1
)
WHERE explain LIKE '%Description: limit%';

DROP TABLE t_exact_rows;
