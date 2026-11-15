-- Float64 cannot represent Int64 values immediately below 2^63. Interpolating DateTime64
-- values near that boundary must preserve the exact samples without converting through Float64.
SELECT quantile(x)
FROM
(
    SELECT fromUnixTimestamp64Nano(if(number % 2 = 0, 9223372036854775807, 9223372036854775806), 'UTC') AS x
    FROM numbers(100)
);

SELECT quantiles(0, 0.5, 1)(x)
FROM
(
    SELECT fromUnixTimestamp64Nano(if(number % 2 = 0, 9223372036854775807, 9223372036854775806), 'UTC') AS x
    FROM numbers(100)
);
