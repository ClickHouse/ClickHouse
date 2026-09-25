-- A large array of almost-identical extreme values plus a few outliers of the opposite
-- extreme: the level-1 (maximum) and level-0 (minimum) quantiles must return the exact
-- extremes even when the outliers are missed by the radix drill-down range sampling.
SELECT quantileExact(1)(x), quantileExact(0)(x)
FROM (SELECT if(number IN (1, 7), 9223372036854775807, -9223372036854775808) AS x FROM numbers(100000));

SELECT quantileExact(1)(x), quantileExact(0)(x)
FROM (SELECT if(number IN (1, 7), 18446744073709551615, 0) :: UInt64 AS x FROM numbers(100000));

SELECT quantilesExactLow(0, 1)(x), quantilesExactHigh(0, 1)(x)
FROM (SELECT if(number IN (1, 7), 9223372036854775807, -9223372036854775808) AS x FROM numbers(100000));
