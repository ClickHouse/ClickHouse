-- Regression: `WITH CLUSTER 0` means exact `GROUP BY` (only identical keys ever merge), so it
-- must work for any key value. The `distance > 0` bucketing needs keys inside Float64's
-- exact-integer range and rejects wide 64-bit ranges (see 04512), but at `distance == 0` no
-- arithmetic happens, so those keys must be bucketed by exact equality instead of being wrongly
-- rejected with `BAD_ARGUMENTS`.
-- See https://github.com/ClickHouse/ClickHouse/pull/101878

SET enable_analyzer = 1; -- `WITH CLUSTER` is implemented for the analyzer only
SET allow_experimental_group_by_with_cluster = 1;

-- 1D `UInt64`: values span 2^60 (>> 2^53). Exact match keeps them as two separate groups.
SELECT 'uint64 wide';
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT x, count() AS c
    FROM VALUES('x UInt64', (0), (1152921504606846976))
    GROUP BY x WITH CLUSTER 0
);

-- Adjacent wide `UInt64` (three values differing by 1 at magnitude 2^60) must stay separate.
SELECT 'uint64 adjacent';
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT x, count() AS c
    FROM VALUES('x UInt64', (1152921504606846976), (1152921504606846977), (1152921504606846978))
    GROUP BY x WITH CLUSTER 0
);

-- 1D `Int64`: full-range negative and positive extremes plus zero.
SELECT 'int64 wide';
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT x, count() AS c
    FROM VALUES('x Int64', (-9223372036854775807), (0), (9223372036854775807))
    GROUP BY x WITH CLUSTER 0
);

-- 1D `DateTime64(9)`: 200 days of nanosecond ticks span more than 2^53.
SELECT 'datetime64 wide';
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT x, count() AS c
    FROM (
        SELECT toDateTime64('2000-01-01 00:00:00', 9) AS x
        UNION ALL SELECT toDateTime64('2000-07-19 00:00:00', 9)
    )
    GROUP BY x WITH CLUSTER 0
);

-- 2D `UInt64` coordinates: exact per-axis match keeps the three distinct points apart.
SELECT '2d uint64 wide';
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT x, y, count() AS c
    FROM (
        SELECT toUInt64(0) AS x, toUInt64(0) AS y
        UNION ALL SELECT toUInt64(1152921504606846976), toUInt64(0)
        UNION ALL SELECT toUInt64(0), toUInt64(1152921504606846976)
    )
    GROUP BY (x, y) WITH CLUSTER 0
);

-- Sanity: the same wide `UInt64` range with `distance > 0` needs Float64 arithmetic and is
-- still rejected (the range guard applies only to the arithmetic path, not to exact matching).
SELECT 'distance > 0 still rejects wide range';
SELECT count() FROM (
    SELECT toUInt64(if(number = 0, 0, 1152921504606846976)) AS x FROM numbers(2)
) GROUP BY x WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }
