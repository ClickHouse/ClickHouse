-- Skewness and kurtosis are location-invariant. The stable variants preserve this
-- under a large offset (1e9) where the naive variants produce NaN due to catastrophic
-- cancellation in the two-pass textbook formula.

-- (a) Translation invariance: |stable(x) - stable(x + 1e9)| < 1e-6
SELECT
    round(abs(skewPopStable(x) - skewPopStable(x + 1000000000)), 6) AS skew_pop_invariance,
    round(abs(skewSampStable(x) - skewSampStable(x + 1000000000)), 6) AS skew_samp_invariance,
    round(abs(kurtPopStable(x) - kurtPopStable(x + 1000000000)), 6) AS kurt_pop_invariance,
    round(abs(kurtSampStable(x) - kurtSampStable(x + 1000000000)), 6) AS kurt_samp_invariance
FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(20000));

-- (b) Parity: on well-conditioned small-integer data, stable matches naive to 8 decimal places
SELECT
    round(abs(skewPopStable(x) - skewPop(x)), 8) AS skew_pop_parity,
    round(abs(skewSampStable(x) - skewSamp(x)), 8) AS skew_samp_parity,
    round(abs(kurtPopStable(x) - kurtPop(x)), 8) AS kurt_pop_parity,
    round(abs(kurtSampStable(x) - kurtSamp(x)), 8) AS kurt_samp_parity
FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(20000));

-- (c) Merge correctness: combining two halves via -State/-Merge equals one-shot aggregation
SELECT
    round(abs(
        (SELECT skewPopStableMerge(s) FROM (
            SELECT skewPopStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(10000))
            UNION ALL
            SELECT skewPopStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(10000, 10000))
        ))
        - (SELECT skewPopStable(x) FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(20000)))
    ), 10) AS skew_pop_merge,
    round(abs(
        (SELECT kurtPopStableMerge(s) FROM (
            SELECT kurtPopStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(10000))
            UNION ALL
            SELECT kurtPopStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(10000, 10000))
        ))
        - (SELECT kurtPopStable(x) FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(20000)))
    ), 10) AS kurt_pop_merge;

-- (c2) Merge correctness for samp variants
SELECT
    round(abs(
        (SELECT skewSampStableMerge(s) FROM (
            SELECT skewSampStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(10000))
            UNION ALL
            SELECT skewSampStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(10000, 10000))
        ))
        - (SELECT skewSampStable(x) FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(20000)))
    ), 10) AS skew_samp_merge,
    round(abs(
        (SELECT kurtSampStableMerge(s) FROM (
            SELECT kurtSampStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(10000))
            UNION ALL
            SELECT kurtSampStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(10000, 10000))
        ))
        - (SELECT kurtSampStable(x) FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(20000)))
    ), 10) AS kurt_samp_merge;

-- (d) Edge cases: empty and singleton return NaN
SELECT isNaN(skewPopStable(x)), isNaN(skewSampStable(x)), isNaN(kurtPopStable(x)), isNaN(kurtSampStable(x))
FROM (SELECT 1 AS x LIMIT 0);

SELECT isNaN(skewPopStable(x)), isNaN(skewSampStable(x)), isNaN(kurtPopStable(x)), isNaN(kurtSampStable(x))
FROM (SELECT 1 AS x);

-- (e) Constant input: m2 = 0 so all four return NaN
SELECT isNaN(skewPopStable(x)), isNaN(skewSampStable(x)), isNaN(kurtPopStable(x)), isNaN(kurtSampStable(x))
FROM (SELECT 5 AS x FROM numbers(100));

-- (f) Asymmetric split (1 vs 19999) exercises the non-comparable branch in mergeWith
SELECT
    round(abs(
        (SELECT skewPopStableMerge(s) FROM (
            SELECT skewPopStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(1))
            UNION ALL
            SELECT skewPopStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(1, 19999))
        ))
        - (SELECT skewPopStable(x) FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(20000)))
    ), 10) AS skew_asymmetric_merge,
    round(abs(
        (SELECT kurtPopStableMerge(s) FROM (
            SELECT kurtPopStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(1))
            UNION ALL
            SELECT kurtPopStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(1, 19999))
        ))
        - (SELECT kurtPopStable(x) FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(20000)))
    ), 10) AS kurt_asymmetric_merge;

-- (g) Two-element sample: count=2 satisfies count>1 so samp variants return a value, not NaN
SELECT isNaN(skewSampStable(x)), isNaN(kurtSampStable(x))
FROM (SELECT number + 1 AS x FROM numbers(2));

-- (h) Scale invariance
SELECT
    round(abs(kurtPopStable(x*3) - kurtPopStable(x)), 10) AS kurt_scale_invariant,
    round(skewPopStable(-x) + skewPopStable(x), 10) AS skew_negation_antisymmetric
FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(20000));

-- (i) Known exact values: integers 1..7 form a symmetric uniform distribution
-- skewPop = 0 exactly, kurtPop = 1.75 exactly
SELECT
    skewPopStable(x) = 0 AS skew_exact_zero,
    kurtPopStable(x) = 1.75 AS kurt_exact_175
FROM (SELECT number + 1 AS x FROM numbers(7));

-- (k) Cross-distribution unequal-size merge: exercises delta-correction terms in mergeWith
-- Two partitions with different means and shapes (300 rows vs 9000 rows) make delta large,
-- so the delta^2/delta^3/delta^4 Pébay correction terms contribute significantly.
-- A wrong coefficient in those terms produces a large error here even if same-distribution
-- splits pass.
SELECT
    round(abs(
        (SELECT skewPopStableMerge(s) FROM (
            SELECT skewPopStableState(x) AS s FROM (SELECT (number % 3) + 1 AS x FROM numbers(300))
            UNION ALL
            SELECT skewPopStableState(x) AS s FROM (SELECT (number % 11) + 1 AS x FROM numbers(9000))
        ))
        - (SELECT skewPopStable(x) FROM (
            SELECT (number % 3) + 1 AS x FROM numbers(300)
            UNION ALL
            SELECT (number % 11) + 1 AS x FROM numbers(9000)
        ))
    ), 10) AS skew_cross_dist_merge,
    round(abs(
        (SELECT skewSampStableMerge(s) FROM (
            SELECT skewSampStableState(x) AS s FROM (SELECT (number % 3) + 1 AS x FROM numbers(300))
            UNION ALL
            SELECT skewSampStableState(x) AS s FROM (SELECT (number % 11) + 1 AS x FROM numbers(9000))
        ))
        - (SELECT skewSampStable(x) FROM (
            SELECT (number % 3) + 1 AS x FROM numbers(300)
            UNION ALL
            SELECT (number % 11) + 1 AS x FROM numbers(9000)
        ))
    ), 10) AS skew_samp_cross_dist_merge,
    round(abs(
        (SELECT kurtPopStableMerge(s) FROM (
            SELECT kurtPopStableState(x) AS s FROM (SELECT (number % 3) + 1 AS x FROM numbers(300))
            UNION ALL
            SELECT kurtPopStableState(x) AS s FROM (SELECT (number % 11) + 1 AS x FROM numbers(9000))
        ))
        - (SELECT kurtPopStable(x) FROM (
            SELECT (number % 3) + 1 AS x FROM numbers(300)
            UNION ALL
            SELECT (number % 11) + 1 AS x FROM numbers(9000)
        ))
    ), 10) AS kurt_cross_dist_merge,
    round(abs(
        (SELECT kurtSampStableMerge(s) FROM (
            SELECT kurtSampStableState(x) AS s FROM (SELECT (number % 3) + 1 AS x FROM numbers(300))
            UNION ALL
            SELECT kurtSampStableState(x) AS s FROM (SELECT (number % 11) + 1 AS x FROM numbers(9000))
        ))
        - (SELECT kurtSampStable(x) FROM (
            SELECT (number % 3) + 1 AS x FROM numbers(300)
            UNION ALL
            SELECT (number % 11) + 1 AS x FROM numbers(9000)
        ))
    ), 10) AS kurt_samp_cross_dist_merge;

-- (j) Three-way merge: three partitions combined equals one-shot aggregation
SELECT
    round(abs(
        (SELECT skewPopStableMerge(s) FROM (
            SELECT skewPopStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(7000))
            UNION ALL
            SELECT skewPopStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(7000, 7000))
            UNION ALL
            SELECT skewPopStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(14000, 6000))
        ))
        - (SELECT skewPopStable(x) FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(20000)))
    ), 10) AS skew_three_way_merge,
    round(abs(
        (SELECT kurtPopStableMerge(s) FROM (
            SELECT kurtPopStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(7000))
            UNION ALL
            SELECT kurtPopStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(7000, 7000))
            UNION ALL
            SELECT kurtPopStableState(x) AS s FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(14000, 6000))
        ))
        - (SELECT kurtPopStable(x) FROM (SELECT (number % 7) + if(number % 13 = 0, 5, 0) AS x FROM numbers(20000)))
    ), 10) AS kurt_three_way_merge;
