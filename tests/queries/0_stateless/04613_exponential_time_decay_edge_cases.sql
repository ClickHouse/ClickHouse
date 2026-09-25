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

-- Empty aggregates use the canonical zero representation.
SELECT
    tupleElement(decaying_sum, 'sign'),
    tupleElement(decaying_sum, 'signed_unit_time') = 0,
    exponentialTimeDecayingDecayLength(decaying_sum),
    isNaN(decaying_avg),
    tupleElement(decaying_count, 'sign'),
    tupleElement(decaying_count, 'signed_unit_time') = 0,
    exponentialTimeDecayingDecayLength(decaying_count)
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
    round(exponentialTimeDecayingValueAt(exponentialTimeDecayedSum(10)(value, time), toFloat64(10)), 6),
    round(exponentialTimeDecayedAvg(10)(value, time), 6),
    round(exponentialTimeDecayingValueAt(exponentialTimeDecayedCount(10)(time), toFloat64(10)), 6)
FROM VALUES(
    'value Nullable(Float64), time Nullable(Float64)',
    (2, 0),
    (NULL, 10),
    (4, NULL),
    (6, 10));

-- A sufficiently old contribution underflows to zero without producing a
-- non-finite result.
SELECT
    exponentialTimeDecayingValueAt(exponentialTimeDecayedSum(1)(value, time), toFloat64(0)),
    exponentialTimeDecayedAvg(1)(value, time),
    exponentialTimeDecayingValueAt(exponentialTimeDecayedCount(1)(time), toFloat64(0))
FROM VALUES('value Float64, time Float64', (1000, -10000), (2, 0));

WITH
    exponentialTimeDecaying(1)(1000, toFloat64(-10000)) AS old_value,
    exponentialTimeDecaying(1)(2, toFloat64(0)) AS current_value,
    old_value + current_value AS combined
SELECT
    exponentialTimeDecayingValueAt(combined, toFloat64(0)),
    toFloat64(0),
    round(exponentialTimeDecayingValueAt(combined, toFloat64(1)), 6);


-- A zero finalized-value cutoff preserves exact behavior.
SET exponential_time_decay_significance_cutoff = 0;
SELECT round(
    exponentialTimeDecayingValueAt(
        exponentialTimeDecayedSum(10)(value, time),
        toFloat64(100)),
    6)
FROM VALUES('value Float64, time Float64', (1000, 0), (2, 100));

-- Raw rows do not carry a calculation index, so the finalized-value cutoff leaves
-- their aggregation exact.
SET exponential_time_decay_significance_cutoff = 5;
SELECT round(
    exponentialTimeDecayingValueAt(
        exponentialTimeDecayedSum(10)(value, time),
        toFloat64(100)),
    6)
FROM VALUES('value Float64, time Float64', (1000, 0), (2, 100));

-- This remains exact as well, even though the old contribution would be outside
-- the configured distance if an index had been supplied.
SELECT round(
    exponentialTimeDecayingValueAt(
        exponentialTimeDecayedSum(10)(value, time),
        toFloat64(100)),
    6)
FROM VALUES('value Float64, time Float64', (1, 0), (2, 100));

SELECT round(
    exponentialTimeDecayingValueAt(
        exponentialTimeDecayedSum(10)(value, time),
        toFloat64(100)),
    6)
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
SELECT round(
    exponentialTimeDecayingValueAt(
        exponentialTimeDecayedSumMerge(10)(state),
        toFloat64(100)),
    6)
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
FROM
(
    SELECT exponentialTimeDecaying(10)(1, toFloat64(0)) AS decaying_value
    UNION ALL
    SELECT exponentialTimeDecaying(10)(2, toFloat64(100)) AS decaying_value
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
        ExponentialTimeDecaying(10))
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
