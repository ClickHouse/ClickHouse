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
    exponentialTimeDecaying(10)(-8, toFloat64(0)) AS a,
    exponentialTimeDecaying(10)(4, toFloat64(10)) AS b,
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
    SELECT exponentialTimeDecaying(10)(v, toFloat64(0)) AS value
    FROM VALUES('v Float64', (-1), (2), (0), (-2), (1))
)
ORDER BY value;

-- Sorting must use the complete curve, not just its value or anchor time.
-- At every common evaluation time, the recent magnitude-2 curves are farther
-- from zero than the old magnitude-100 curves in this example.
SELECT id
FROM
(
    SELECT
        id,
        exponentialTimeDecaying(10)(value, time) AS decaying_value
    FROM VALUES(
        'id UInt8, value Float64, time Float64',
        (1, -100, 0),
        (2, -2, 45),
        (3, 0, -1000),
        (4, 100, 0),
        (5, 2, 45))
)
ORDER BY decaying_value, id;

SELECT id
FROM
(
    SELECT
        id,
        exponentialTimeDecaying(10)(value, time) AS decaying_value
    FROM VALUES(
        'id UInt8, value Float64, time Float64',
        (1, -100, 0),
        (2, -2, 45),
        (3, 0, -1000),
        (4, 100, 0),
        (5, 2, 45))
)
ORDER BY decaying_value DESC, id;

WITH
    exponentialTimeDecaying(10)(100, toFloat64(0)) AS old_positive,
    exponentialTimeDecaying(10)(2, toFloat64(45)) AS recent_positive,
    exponentialTimeDecaying(10)(-100, toFloat64(0)) AS old_negative,
    exponentialTimeDecaying(10)(-2, toFloat64(45)) AS recent_negative
SELECT
    old_positive < recent_positive,
    old_positive <= recent_positive,
    old_positive > recent_positive,
    old_positive >= recent_positive,
    recent_negative < old_negative,
    recent_negative <= old_negative,
    recent_negative > old_negative,
    recent_negative >= old_negative;

-- Equal curves reconstructed at different anchor times have the same sort key.
WITH
    exponentialTimeDecaying(10)(2, toFloat64(0)) AS a,
    exponentialTimeDecaying(10)(1, toFloat64(10 * log(2))) AS b
SELECT a = b, a <= b, a >= b, a < b, a > b;

WITH
    exponentialTimeDecaying(10)(-2, toFloat64(0)) AS a,
    exponentialTimeDecaying(10)(-1, toFloat64(0)) AS b
SELECT a < b, a <= b, a > b, a >= b, a = b, a != b;

SELECT
    exponentialTimeDecaying(10)(1, toFloat64(0))
    < exponentialTimeDecaying(20)(1, toFloat64(0)); -- { serverError BAD_ARGUMENTS, ILLEGAL_TYPE_OF_ARGUMENT }

-- The compact UInt64 ordering prefix intentionally merges neighboring Float64
-- unit timestamps that differ only in the discarded low-order bit. A prefix
-- collision must not become logical equality or a logical hash collision.
WITH
    reinterpretAsFloat64(reinterpretAsUInt64(toFloat64(1)) + 1) AS t1,
    reinterpretAsFloat64(reinterpretAsUInt64(toFloat64(1)) + 2) AS t2,
    exponentialTimeDecaying(1)(1., t1) AS a,
    exponentialTimeDecaying(1)(1., t2) AS b
SELECT
    a = b,
    a < b,
    a > b,
    cityHash64(a) = cityHash64(b);

-- Serialized hash-table keys must retain the full logical key too.
SELECT count()
FROM
(
    SELECT value
    FROM
    (
        SELECT exponentialTimeDecaying(1)(
            1.,
            reinterpretAsFloat64(reinterpretAsUInt64(toFloat64(1)) + 1)) AS value
        UNION ALL
        SELECT exponentialTimeDecaying(1)(
            1.,
            reinterpretAsFloat64(reinterpretAsUInt64(toFloat64(1)) + 2)) AS value
    )
    GROUP BY value
);

-- Re-anchored representations of the same curve still have one logical key.
SELECT count()
FROM
(
    SELECT value
    FROM
    (
        SELECT exponentialTimeDecaying(10)(2., toFloat64(0)) AS value
        UNION ALL
        SELECT exponentialTimeDecaying(10)(1., toFloat64(10 * log(2))) AS value
    )
    GROUP BY value
);



-- Equivalent negative curves reconstructed at different anchors must remain one
-- logical value, including equality, ordering, hashing, and serialized GROUP BY keys.
WITH
    exponentialTimeDecaying(10)(-2., toFloat64(0)) AS a,
    exponentialTimeDecaying(10)(-1., toFloat64(10 * log(2))) AS b
SELECT 'negative re-anchoring mismatch'
WHERE NOT (
    a = b
    AND a <= b
    AND a >= b
    AND NOT (a < b)
    AND NOT (a > b)
    AND cityHash64(a) = cityHash64(b));

SELECT 'negative re-anchored GROUP BY mismatch'
WHERE
(
    SELECT count()
    FROM
    (
        SELECT value
        FROM
        (
            SELECT exponentialTimeDecaying(10)(-2., toFloat64(0)) AS value
            UNION ALL
            SELECT exponentialTimeDecaying(10)(-1., toFloat64(10 * log(2))) AS value
        )
        GROUP BY value
    )
) != 1;

-- Negative prefixes discard the low sortable-timestamp bit in the opposite
-- ordering domain. Adjacent timestamps below share one compact UInt64 prefix,
-- but the full logical-key fallback must keep them distinct and reverse their
-- order correctly for negative curves.
WITH
    toFloat64(1) AS t1,
    reinterpretAsFloat64(reinterpretAsUInt64(toFloat64(1)) + 1) AS t2,
    exponentialTimeDecaying(1)(-1., t1) AS a,
    exponentialTimeDecaying(1)(-1., t2) AS b
SELECT 'negative prefix-collision mismatch'
WHERE
    a = b
    OR a < b
    OR NOT (a > b)
    OR cityHash64(a) = cityHash64(b);

SELECT 'negative prefix-collision GROUP BY mismatch'
WHERE
(
    SELECT count()
    FROM
    (
        SELECT value
        FROM
        (
            SELECT exponentialTimeDecaying(1)(-1., toFloat64(1)) AS value
            UNION ALL
            SELECT exponentialTimeDecaying(1)(
                -1.,
                reinterpretAsFloat64(reinterpretAsUInt64(toFloat64(1)) + 1)) AS value
        )
        GROUP BY value
    )
) != 2;

-- Exact opposite curves cancel to the canonical zero representation, which
-- must remain ordered strictly between the negative and positive domains.
WITH
    exponentialTimeDecaying(10)(2., toFloat64(7)) AS positive,
    exponentialTimeDecaying(10)(-2., toFloat64(7)) AS negative,
    positive + negative AS cancelled,
    exponentialTimeDecaying(10)(0., toFloat64(123)) AS zero
SELECT 'signed cancellation mismatch'
WHERE NOT (
    cancelled = zero
    AND cityHash64(cancelled) = cityHash64(zero)
    AND tupleElement(cancelled, 'value_at_anchor') = 0
    AND tupleElement(cancelled, 'anchor_time') = 0
    AND exponentialTimeDecaying(10)(-1., toFloat64(0)) < cancelled
    AND cancelled < exponentialTimeDecaying(10)(1., toFloat64(0)));

-- The finalized-value significance cutoff uses the compact index for negative
-- curves too. A contribution more than five decay lengths behind must be
-- discarded symmetrically with the positive domain.
SET exponential_time_decay_significance_cutoff = 5;

SELECT 'negative significance-cutoff mismatch'
WHERE abs(
    (
        SELECT exponentialTimeDecayingValueAt(
            exponentialTimeDecayedSum(value),
            toFloat64(100))
        FROM VALUES(
            'value ExponentialTimeDecaying(10)',
            ((-1., 0., 10.)),
            ((-2., 100., 10.)))
    ) + 2) > 1e-12;

SET exponential_time_decay_significance_cutoff = 0;

-- Signed primary-key and minmax pruning must preserve the same total ordering
-- as row-level comparison across negative, zero, and positive curves.
DROP TABLE IF EXISTS time_decay_signed_index;
CREATE TABLE time_decay_signed_index
(
    id UInt8,
    value ExponentialTimeDecaying(1),
    INDEX value_minmax value TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY value
SETTINGS index_granularity = 1;

INSERT INTO time_decay_signed_index
SELECT *
FROM VALUES(
    'id UInt8, value ExponentialTimeDecaying(1)',
    (1, (-1., 0., 1.)),
    (2, (-1., 1., 1.)),
    (3, (0., 0., 1.)),
    (4, (1., 0., 1.)),
    (5, (1., 1., 1.)));

SELECT 'signed negative-domain index mismatch'
WHERE
(
    SELECT count()
    FROM time_decay_signed_index
    WHERE value < exponentialTimeDecaying(1)(-1., toFloat64(0))
    SETTINGS
        force_primary_key = 1,
        force_data_skipping_indices = 'value_minmax'
) != 1;

SELECT 'signed zero-boundary index mismatch'
WHERE
(
    SELECT count()
    FROM time_decay_signed_index
    WHERE value < exponentialTimeDecaying(1)(0., toFloat64(0))
    SETTINGS
        force_primary_key = 1,
        force_data_skipping_indices = 'value_minmax'
) != 2;

SELECT 'signed positive-domain index mismatch'
WHERE
(
    SELECT count()
    FROM time_decay_signed_index
    WHERE value > exponentialTimeDecaying(1)(0., toFloat64(0))
    SETTINGS
        force_primary_key = 1,
        force_data_skipping_indices = 'value_minmax'
) != 2;

DROP TABLE time_decay_signed_index;


-- Unit-time inspection exposes the compact ordering bucket and the residual
-- amplitude retained by the authoritative direct curve. The discarded low bit
-- must not be silently rounded into an exact +/-1 value.
WITH
    reinterpretAsFloat64(reinterpretAsUInt64(toFloat64(1)) + 1) AS exact_unit_time,
    reinterpretAsFloat64(reinterpretAsUInt64(toFloat64(1)) + 2) AS positive_bucket_time,
    exponentialTimeDecaying(1)(1., exact_unit_time) AS positive,
    exponentialTimeDecaying(1)(-1., exact_unit_time) AS negative
SELECT 'unit-time residual mismatch'
WHERE NOT (
    exponentialTimeDecayingUnitTime(positive) = positive_bucket_time
    AND exponentialTimeDecayingValueAtUnitTime(positive)
        = exponentialTimeDecayingValueAt(
            positive,
            exponentialTimeDecayingUnitTime(positive))
    AND exponentialTimeDecayingValueAtUnitTime(positive) < 1
    AND exponentialTimeDecayingValueAtUnitTime(positive) > 0.999999999999999
    AND exponentialTimeDecayingUnitTime(negative) = toFloat64(1)
    AND exponentialTimeDecayingValueAtUnitTime(negative)
        = exponentialTimeDecayingValueAt(
            negative,
            exponentialTimeDecayingUnitTime(negative))
    AND exponentialTimeDecayingValueAtUnitTime(negative) < -1
    AND exponentialTimeDecayingValueAtUnitTime(negative) > -1.000000000000001);

-- Within one compact unit-time bucket, the residual value carries the remaining
-- ordering precision. The direction must agree with native curve ordering in
-- both sign domains.
WITH
    reinterpretAsFloat64(reinterpretAsUInt64(toFloat64(1)) + 1) AS t1,
    reinterpretAsFloat64(reinterpretAsUInt64(toFloat64(1)) + 2) AS t2,
    exponentialTimeDecaying(1)(1., t1) AS positive_a,
    exponentialTimeDecaying(1)(1., t2) AS positive_b,
    exponentialTimeDecaying(1)(-1., toFloat64(1)) AS negative_a,
    exponentialTimeDecaying(1)(-1., t1) AS negative_b
SELECT 'unit-time pair ordering mismatch'
WHERE NOT (
    exponentialTimeDecayingUnitTime(positive_a)
        = exponentialTimeDecayingUnitTime(positive_b)
    AND exponentialTimeDecayingValueAtUnitTime(positive_a)
        < exponentialTimeDecayingValueAtUnitTime(positive_b)
    AND positive_a < positive_b
    AND exponentialTimeDecayingUnitTime(negative_a)
        = exponentialTimeDecayingUnitTime(negative_b)
    AND exponentialTimeDecayingValueAtUnitTime(negative_a)
        > exponentialTimeDecayingValueAtUnitTime(negative_b)
    AND negative_a > negative_b);

-- Equivalent curves must expose the same ordered pair regardless of the anchor
-- used to construct them.
WITH
    exponentialTimeDecaying(10)(2., toFloat64(0)) AS positive_a,
    exponentialTimeDecaying(10)(1., toFloat64(10 * log(2))) AS positive_b,
    exponentialTimeDecaying(10)(-2., toFloat64(0)) AS negative_a,
    exponentialTimeDecaying(10)(-1., toFloat64(10 * log(2))) AS negative_b
SELECT 'unit-time re-anchoring mismatch'
WHERE NOT (
    exponentialTimeDecayingUnitTime(positive_a)
        = exponentialTimeDecayingUnitTime(positive_b)
    AND exponentialTimeDecayingValueAtUnitTime(positive_a)
        = exponentialTimeDecayingValueAtUnitTime(positive_b)
    AND exponentialTimeDecayingUnitTime(negative_a)
        = exponentialTimeDecayingUnitTime(negative_b)
    AND exponentialTimeDecayingValueAtUnitTime(negative_a)
        = exponentialTimeDecayingValueAtUnitTime(negative_b));

-- Zero remains the midpoint identity for the exposed ordering pair.
WITH exponentialTimeDecaying(10)(0., toFloat64(123)) AS zero
SELECT 'zero unit-time mismatch'
WHERE NOT (
    exponentialTimeDecayingUnitTime(zero) = 0
    AND exponentialTimeDecayingValueAtUnitTime(zero) = 0);

-- The inspection functions accept only the decaying type.
SELECT exponentialTimeDecayingUnitTime(toFloat64(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT exponentialTimeDecayingValueAtUnitTime(toFloat64(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
