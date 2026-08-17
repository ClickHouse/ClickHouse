SET allow_experimental_time_decay_aggregate_functions = 1;

-- The aggregate forms preserve signed values.
SELECT
    round(exponentialTimeDecayingValueAt(exponentialTimeDecayedSum(10)(value, time), toFloat64(10)), 6),
    round(exponentialTimeDecayedAvg(10)(value, time), 6)
FROM VALUES('value Float64, time Float64', (-10, 0), (20, 10), (-5, 5));

-- Signed partial states remain independent of batch distribution.
WITH
    direct AS
    (
        SELECT
            exponentialTimeDecayingValueAt(exponentialTimeDecayedSum(10)(value, time), toFloat64(10)) AS sum,
            exponentialTimeDecayedAvg(10)(value, time) AS avg
        FROM VALUES('value Float64, time Float64', (-10, 0), (20, 10), (-5, 5))
    ),
    merged AS
    (
        SELECT
            exponentialTimeDecayingValueAt(exponentialTimeDecayedSumMerge(10)(sum_state), toFloat64(10)) AS sum,
            exponentialTimeDecayedAvgMerge(10)(avg_state) AS avg
        FROM
        (
            SELECT
                exponentialTimeDecayedSumState(10)(value, time) AS sum_state,
                exponentialTimeDecayedAvgState(10)(value, time) AS avg_state
            FROM VALUES('value Float64, time Float64', (-10, 0), (-5, 5))
            UNION ALL
            SELECT
                exponentialTimeDecayedSumState(10)(value, time) AS sum_state,
                exponentialTimeDecayedAvgState(10)(value, time) AS avg_state
            FROM VALUES('value Float64, time Float64', (20, 10))
        )
    )
SELECT
    abs(direct.sum - merged.sum) <= 1e-12 * greatest(1., abs(direct.sum)),
    abs(direct.avg - merged.avg) <= 1e-12 * greatest(1., abs(direct.avg))
FROM direct
CROSS JOIN merged;

-- The value type represents signed decay curves and addition preserves cancellation.
WITH
    exponentialTimeDecayingFloat64(10)(-8, toFloat64(0)) AS a,
    exponentialTimeDecayingFloat64(10)(4, toFloat64(10)) AS b,
    a + b AS c
SELECT
    toTypeName(c),
    round(exponentialTimeDecayingValueAt(c, toFloat64(10)), 6),
    toFloat64(10),
    round(exponentialTimeDecayingDecayLength(c), 6),
    round(exponentialTimeDecayingValueAt(c, toFloat64(20)), 6);

-- The canonical representation makes the regular tuple sorter order curves
-- by their numeric value at every common evaluation time.
SELECT round(exponentialTimeDecayingValueAt(value, toFloat64(0)), 6)
FROM
(
    SELECT exponentialTimeDecayingFloat64(10)(v, toFloat64(0)) AS value
    FROM VALUES('v Float64', (-1), (2), (0), (-2), (1))
)
ORDER BY value;

WITH
    exponentialTimeDecayingFloat64(10)(-2, toFloat64(0)) AS a,
    exponentialTimeDecayingFloat64(10)(-1, toFloat64(0)) AS b
SELECT a < b, a <= b, a > b, a >= b, a = b, a != b;

SELECT
    exponentialTimeDecayingFloat64(10)(1, toFloat64(0))
    < exponentialTimeDecayingFloat64(20)(1, toFloat64(0)); -- { serverError BAD_ARGUMENTS }
