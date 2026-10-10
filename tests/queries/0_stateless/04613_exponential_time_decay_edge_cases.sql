SET allow_experimental_time_decay_aggregate_functions = 1;

-- Keep the aggregate-state alignment regression out of the legacy window test.
-- The combination of differently aligned aggregate states previously exposed a UBSan failure.
SELECT 'unaligned aggregate state regression'
FROM VALUES('id Int8, time DateTime', (1,1),(1,2),(2,3),(3,3),(3,5))
HAVING NOT (
    anyLast(id) = 3
    AND toUInt32(anyLast(time)) = 5
    AND isFinite(exponentialTimeDecayedAvg(10)(id, time))
);

-- Empty raw aggregates finalize to their raw numeric defaults.
SELECT
    decaying_sum,
    isNaN(decaying_avg),
    decaying_count,
    toTypeName(decaying_sum),
    toTypeName(decaying_count)
FROM
(
    SELECT
        exponentialTimeDecayedSum(10)(value, time) AS decaying_sum,
        exponentialTimeDecayedAvg(10)(value, time) AS decaying_avg,
        exponentialTimeDecayedCount(10)(time) AS decaying_count
    FROM VALUES('value Float64, time Float64', (1, 1))
    WHERE false
);

-- The OrNull combinator distinguishes an empty aggregate from the regular default.
SELECT
    isNull(exponentialTimeDecayedSumOrNull(10)(value, time)),
    isNull(exponentialTimeDecayedAvgOrNull(10)(value, time)),
    isNull(exponentialTimeDecayedCountOrNull(10)(time))
FROM VALUES('value Float64, time Float64', (1, 1))
WHERE false;

-- Nullable aggregate arguments skip rows containing NULL in an argument used by
-- that aggregate. Count only depends on time, so a NULL value does not skip it.
SELECT
    round(exponentialTimeDecayedSum(10)(value, time), 6),
    round(exponentialTimeDecayedAvg(10)(value, time), 6),
    round(exponentialTimeDecayedCount(10)(time), 6)
FROM VALUES(
    'value Nullable(Float64), time Nullable(Float64)',
    (2, 0),
    (NULL, 10),
    (4, NULL),
    (6, 10));

-- A sufficiently old contribution underflows to zero without producing a
-- non-finite result.
SELECT
    exponentialTimeDecayedSum(1)(value, time),
    exponentialTimeDecayedAvg(1)(value, time),
    exponentialTimeDecayedCount(1)(time)
FROM VALUES('value Float64, time Float64', (1000, -10000), (2, 0));

WITH
    exponentialTimeDecaying(1)(1000, toFloat64(-10000)) AS old_value,
    exponentialTimeDecaying(1)(2, toFloat64(0)) AS current_value,
    old_value + current_value AS combined
SELECT
    combined = current_value,
    exponentialTimeDecayingValueAt(combined, toFloat64(0))
        = exponentialTimeDecayingValueAt(current_value, toFloat64(0)),
    round(exponentialTimeDecayingValueAt(combined, toFloat64(1)), 6);

-- Clamp a derived unit timestamp outside the finite `Float64` range.
-- The 64-bit key discards the low sortable timestamp bit, so the positive
-- boundary decodes to the `Float64` immediately below the largest finite value.
WITH
    exponentialTimeDecaying(1e308)(1e308, toFloat64(0)) AS upper_curve,
    exponentialTimeDecaying(1e308)(1e-308, toFloat64(0)) AS lower_curve
SELECT
    exponentialTimeDecayingUnitTime(upper_curve) = toFloat64('1.7976931348623155e308'),
    exponentialTimeDecayingUnitTime(lower_curve) = -toFloat64('1.7976931348623157e308'),
    exponentialTimeDecayingValueAtUnitTime(upper_curve) = 1,
    exponentialTimeDecayingValueAtUnitTime(lower_curve) = 1;

-- Addition remains valid when the resulting curve has a finite key, even if
-- evaluating it at the original input timestamp overflows `Float64`.
WITH
    exponentialTimeDecaying(1)(1e308, toFloat64(0)) AS value,
    value + value AS combined
SELECT
    combined > value,
    isFinite(exponentialTimeDecayingUnitTime(combined)),
    round(exponentialTimeDecayingValueAtUnitTime(combined), 10) = 1,
    round(exponentialTimeDecayingValueAt(combined, exponentialTimeDecayingUnitTime(value)), 10) = 2,
    isInfinite(exponentialTimeDecayingValueAt(combined, toFloat64(0)));

-- Raw numeric aggregate finalization still rejects `Float64` overflow.
SELECT exponentialTimeDecayedSum(1)(value, time)
FROM VALUES('value Float64, time Float64', (1e308, 0), (1e308, 0)); -- { serverError BAD_ARGUMENTS }


-- A zero finalized-value cutoff preserves exact behavior.
SET exponential_time_decay_significance_cutoff = 0;
SELECT round(exponentialTimeDecayedSum(10)(value, time), 6)
FROM VALUES('value Float64, time Float64', (1000, 0), (2, 100));

-- Raw rows do not carry a calculation index, so the finalized-value cutoff leaves
-- their aggregation exact.
SET exponential_time_decay_significance_cutoff = 5;
SELECT round(exponentialTimeDecayedSum(10)(value, time), 6)
FROM VALUES('value Float64, time Float64', (1000, 0), (2, 100));

-- This remains exact as well, even though the old contribution would be outside
-- the configured distance if an index had been supplied.
SELECT round(exponentialTimeDecayedSum(10)(value, time), 6)
FROM VALUES('value Float64, time Float64', (1, 0), (2, 100));

SELECT round(exponentialTimeDecayedSum(10)(value, time), 6)
FROM VALUES('value Float64, time Float64', (100, 60), (2, 100));

-- An average keeps a state when either its numerator or denominator is still
-- significant; a large old value must not be dropped based on age alone.
WITH
    exponentialTimeDecayedAvg(10)(value, time) AS actual,
    (1000000 * exp(-10) + 2) / (exp(-10) + 1) AS expected
SELECT abs(actual - expected) <= 1e-12 * greatest(1., abs(expected))
FROM VALUES('value Float64, time Float64', (1000000, 0), (2, 100));

-- Aggregate-state merges remain exact. In particular, storage-engine merges do
-- not inherit a query's finalized-value cutoff.
SELECT round(exponentialTimeDecayedSumMerge(10)(state), 6)
FROM
(
    SELECT exponentialTimeDecayedSumState(10)(value, time) AS state
    FROM VALUES('value Float64, time Float64', (1, 0))
    UNION ALL
    SELECT exponentialTimeDecayedSumState(10)(value, time) AS state
    FROM VALUES('value Float64, time Float64', (2, 100))
);

-- Finalized values already carry the unit-magnitude timestamp. This is the only
-- input form on which this cutoff is applied; sorting these values by their
-- calculation-index timestamp makes the fast rejection path especially effective.
SELECT round(
    exponentialTimeDecayingValueAt(
        exponentialTimeDecayedSum(decaying_value),
        toFloat64(100)),
    6)
FROM VALUES(
    'decaying_value ExponentialTimeDecaying64(10)',
    ((1., 0., 10.)),
    ((2., 100., 10.))
);

-- The cutoff applies while finalized rows are added, not while aggregate states
-- are merged. Each one-row state is exact and their later merge must remain exact.
SELECT round(
    exponentialTimeDecayingValueAt(
        exponentialTimeDecayedSumMerge(state),
        toFloat64(100)),
    6)
FROM
(
    SELECT exponentialTimeDecayedSumState(decaying_value) AS state
    FROM
    (
        SELECT exponentialTimeDecaying(10)(1, toFloat64(0)) AS decaying_value
    )
    UNION ALL
    SELECT exponentialTimeDecayedSumState(decaying_value) AS state
    FROM
    (
        SELECT exponentialTimeDecaying(10)(2, toFloat64(100)) AS decaying_value
    )
);

-- The same setting must never approximate persisted SimpleAggregateFunction
-- values during an AggregatingMergeTree background merge.
DROP TABLE IF EXISTS time_decay_budget_engine_exact;
CREATE TABLE time_decay_budget_engine_exact
(
    key UInt8,
    value SimpleAggregateFunction(
        exponentialTimeDecayedSum,
        ExponentialTimeDecaying64(10))
)
ENGINE = AggregatingMergeTree
ORDER BY key;
INSERT INTO time_decay_budget_engine_exact
SELECT 1, exponentialTimeDecaying(10)(1, toFloat64(0));
INSERT INTO time_decay_budget_engine_exact
SELECT 1, exponentialTimeDecaying(10)(2, toFloat64(100));
OPTIMIZE TABLE time_decay_budget_engine_exact FINAL;
SELECT round(exponentialTimeDecayingValueAt(value, toFloat64(100)), 6)
FROM time_decay_budget_engine_exact;
DROP TABLE time_decay_budget_engine_exact;

SET exponential_time_decay_significance_cutoff = -1;
SELECT exponentialTimeDecayedSum(10)(value, time)
FROM VALUES('value Float64, time Float64', (1, 0)); -- { serverError BAD_ARGUMENTS }

SET exponential_time_decay_significance_cutoff = 0;
