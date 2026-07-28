-- Repro: statistics-driven PREWHERE reordering can cause DECIMAL_OVERFLOW.
--
-- Workaround: `allow_reorder_prewhere_conditions = 0` keeps the original WHERE order
-- so the NOT-IN guard stays at position 0.
--
-- Tags: no-fasttest

DROP TABLE IF EXISTS test_prewhere_decimal_overflow;

CREATE TABLE test_prewhere_decimal_overflow
(
    id              UInt32,
    sig             String,
    ts              DateTime,
    balance_changes Array(Tuple(account String, before Decimal(38, 9), after Decimal(38, 9)))
)
ENGINE = MergeTree()
ORDER BY (ts, id)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, auto_statistics_types = '';

-- Single INSERT so all rows land in one part (needed for selectivity estimates to differ).
INSERT INTO test_prewhere_decimal_overflow VALUES
    (1, 'good',    '2024-01-01 10:00:00',
     [('acct1', toDecimal128('1000000000', 9), toDecimal128('2000000000', 9))]),
    (2, 'neutral', '2024-01-01 11:00:00',
     [('acct1', toDecimal128('0', 9), toDecimal128('0', 9))]),
    (3, 'neutral', '2024-01-01 12:00:00',
     [('acct1', toDecimal128('0', 9), toDecimal128('0', 9))]),
    (4, 'bad',     '2024-01-01 13:00:00',
     [('acct1',
       toDecimal128('-90000000000000000000000000000', 9),
       toDecimal128( '90000000000000000000000000000', 9))]);

ALTER TABLE test_prewhere_decimal_overflow ADD STATISTICS ts TYPE minmax;
ALTER TABLE test_prewhere_decimal_overflow MATERIALIZE STATISTICS ts SETTINGS mutations_sync = 1;

SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

-- Correct result without statistics.
SELECT sig
FROM test_prewhere_decimal_overflow
WHERE (ts >= '2024-01-01 00:00:00') AND (ts < '2024-01-02 00:00:00')
    AND (sig NOT IN ('bad'))
    AND (round(arraySum(x -> if(x.account = 'acct1' AND x.after > x.before,
                                 toFloat64(x.after - x.before), toFloat64(0)),
                        balance_changes) / 1000000000) > 0)
SETTINGS use_statistics = 0, allow_experimental_statistics = 1, allow_reorder_prewhere_conditions = 0, query_plan_merge_filters = 1; -- CI may inject merge_filters=False, separating NOT-IN and arraySum into disconnected filter nodes so prewhere can reorder them independently

-- Bug: statistics move `arraySum` before the NOT-IN guard → DECIMAL_OVERFLOW.
SELECT sig
FROM test_prewhere_decimal_overflow
WHERE (ts >= '2024-01-01 00:00:00') AND (ts < '2024-01-02 00:00:00')
    AND (sig NOT IN ('bad'))
    AND (round(arraySum(x -> if(x.account = 'acct1' AND x.after > x.before,
                                 toFloat64(x.after - x.before), toFloat64(0)),
                        balance_changes) / 1000000000) > 0)
SETTINGS use_statistics = 1, allow_experimental_statistics = 1, allow_reorder_prewhere_conditions = 1; -- { serverError DECIMAL_OVERFLOW }

-- Workaround: disable reordering to preserve the original WHERE order.
SELECT sig
FROM test_prewhere_decimal_overflow
WHERE (ts >= '2024-01-01 00:00:00') AND (ts < '2024-01-02 00:00:00')
    AND (sig NOT IN ('bad'))
    AND (round(arraySum(x -> if(x.account = 'acct1' AND x.after > x.before,
                                 toFloat64(x.after - x.before), toFloat64(0)),
                        balance_changes) / 1000000000) > 0)
SETTINGS use_statistics = 1, allow_experimental_statistics = 1, allow_reorder_prewhere_conditions = 0, query_plan_merge_filters = 1; -- CI may inject merge_filters=False, separating NOT-IN and arraySum into disconnected filter nodes so prewhere can reorder them independently

DROP TABLE test_prewhere_decimal_overflow;
