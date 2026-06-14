-- Test for issue: https://github.com/ClickHouse/ClickHouse/issues/97489
-- NOT_FOUND_COLUMN_IN_BLOCK when using UNION ALL with CTEs and a table with skip indexes.
-- The outer WHERE pushes a constant-false predicate into the Medians UNION ALL branch.
-- filterPartsByVirtualColumns must not ignore constant-false predicates. Instead, it should
-- return an empty part set so that no data is read from that branch.

DROP TABLE IF EXISTS test_97489;

CREATE TABLE test_97489 (
    col1 Nullable(String),
    currency Nullable(String),
    INDEX idx_currency currency TYPE set(0) GRANULARITY 4
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS index_granularity = 8192;

-- 1000 rows ensures column sizes are non-zero so that optimizePrewhere runs
-- and moves col1 IS NOT NULL into PREWHERE (triggering the bug on unfixed builds).
INSERT INTO test_97489 SELECT toString(number), toString(number % 100) FROM numbers(1000);

-- The Medians branch has a constant-false condition pushed down from the outer WHERE
-- (calculation_type = 'Median' != 'Raw Totals'). Before the fix this threw:
--   NOT_FOUND_COLUMN_IN_BLOCK: Not found column col1 in block
-- The correct result is 1000 rows from RawTotals and 0 rows from Medians.
WITH RawTotals AS (
    SELECT col1 AS cta FROM test_97489
),
Medians AS (
    SELECT col1 AS cta FROM test_97489 WHERE col1 IS NOT NULL
)
SELECT count()
FROM (
    SELECT *, 'Raw Totals' AS calculation_type FROM RawTotals
    UNION ALL
    SELECT *, 'Median' AS calculation_type FROM Medians
) combined
WHERE calculation_type = 'Raw Totals';

DROP TABLE test_97489;
