-- Regression: 64-bit integer cluster keys lost precision when implicitly cast
-- to Float64 (mantissa is only 53 bits), which could fail to merge adjacent
-- integer values past 2^53. Translation by the column minimum before the
-- Float64 cast preserves all pairwise differences and keeps the math exact
-- while the value range fits 2^53.

-- 1D UInt64 around 2^53: three consecutive integers must merge under d=1.
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT x, count() AS c
    FROM (
        SELECT toUInt64(9007199254740992 + number) AS x FROM numbers(3)
    )
    GROUP BY x WITH CLUSTER 1
);

-- 1D Int64 near INT64_MIN: same, narrow range fits after translation.
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT x, count() AS c
    FROM (
        SELECT toInt64(-9223372036854775000 + number) AS x FROM numbers(3)
    )
    GROUP BY x WITH CLUSTER 1
);

-- 2D UInt64: chain merge via cells must not be broken by Float64 rounding.
SELECT count() AS num_clusters
FROM (
    SELECT x, y, count() AS c
    FROM (
        SELECT toUInt64(9007199254740992 + number) AS x,
               toUInt64(9007199254740992 + number) AS y
        FROM numbers(3)
    )
    GROUP BY (x, y) WITH CLUSTER 2
);

-- Range exceeds Float64 exact-integer limit (2^60 spread) — rejected.
SELECT count() FROM (
    SELECT toUInt64(if(number = 0, 0, 1152921504606846976)) AS x FROM numbers(2)
)
GROUP BY x WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }
