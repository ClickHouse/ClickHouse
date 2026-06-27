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

-- (d) Edge cases: empty and singleton return NaN
SELECT isNaN(skewPopStable(x)), isNaN(skewSampStable(x)), isNaN(kurtPopStable(x)), isNaN(kurtSampStable(x))
FROM (SELECT 1 AS x LIMIT 0);

SELECT isNaN(skewPopStable(x)), isNaN(skewSampStable(x)), isNaN(kurtPopStable(x)), isNaN(kurtSampStable(x))
FROM (SELECT 1 AS x);
