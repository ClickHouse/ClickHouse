SET allow_experimental_time_decay_aggregate_functions = 1;

-- The aggregate forms preserve signed values.
SELECT
    round(tupleElement(exponentialTimeDecayedSum(10)(value, time), 'value'), 6),
    round(exponentialTimeDecayedAvg(10)(value, time), 6)
FROM VALUES('value Float64, time Float64', (-10, 0), (20, 10), (-5, 5));

-- Signed partial states remain independent of batch distribution.
WITH
    direct AS
    (
        SELECT
            tupleElement(exponentialTimeDecayedSum(10)(value, time), 'value') AS sum,
            exponentialTimeDecayedAvg(10)(value, time) AS avg
        FROM VALUES('value Float64, time Float64', (-10, 0), (20, 10), (-5, 5))
    ),
    merged AS
    (
        SELECT
            tupleElement(exponentialTimeDecayedSumMerge(10)(sum_state), 'value') AS sum,
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
    round(tupleElement(c, 'value'), 6),
    tupleElement(c, 'time'),
    round(exponentialTimeDecayingDecayLength(c), 6),
    round(exponentialTimeDecayingValueAt(c, toFloat64(20)), 6);
