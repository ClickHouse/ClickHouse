-- Tags: no-fasttest

-- Regression test for the sliding window frame moving-aggregate optimization in
-- WindowTransform::updateAggregationState(): sum/count/avg/min/max should give the same
-- result as before, whether or not the frame start had to be recomputed incrementally.
-- We cross-check them against arraySum/length/arrayAvg/arrayMin/arrayMax over
-- groupArray(...) OVER the same frame, since groupArray does not support the
-- moving-aggregate optimization and always goes through the original full-reset path.

DROP TABLE IF EXISTS moving_aggregate_test;
CREATE TABLE moving_aggregate_test
(
    n UInt32,
    part UInt8,
    value Int64,
    nullable_value Nullable(Int64),
    str String,
    fvalue Float64
) ENGINE = Memory;

INSERT INTO moving_aggregate_test
SELECT
    number,
    number % 7,
    (cityHash64(number) % 201) - 100,
    if(number % 11 = 0, NULL, (cityHash64(number, 1) % 201) - 100),
    toString(cityHash64(number, 2) % 5),
    -- Division by 8 keeps the values exactly representable, so float sums are
    -- order-independent and comparable exactly.
    multiIf(number = 500, inf, number = 600, -inf, number = 700, nan, (toInt64(cityHash64(number, 3) % 201) - 100) / 8)
FROM numbers(1000);

SELECT
    frame_size,
    countIf(NOT (
        s = s2 AND c = c2 AND (a = a2 OR (isNaN(a) AND isNaN(a2))) AND mn = mn2 AND mx = mx2
        AND (ns = ns2 OR (isNaN(ns) AND isNaN(ns2))) AND nc = nc2
        AND smn = smn2 AND smx = smx2
    )) AS mismatches
FROM
(
    SELECT
        0 AS frame_size, n,
        sum(value) OVER w AS s, arraySum(groupArray(value) OVER w) AS s2,
        count(value) OVER w AS c, length(groupArray(value) OVER w) AS c2,
        avg(value) OVER w AS a, arrayAvg(groupArray(value) OVER w) AS a2,
        min(value) OVER w AS mn, arrayMin(groupArray(value) OVER w) AS mn2,
        max(value) OVER w AS mx, arrayMax(groupArray(value) OVER w) AS mx2,
        sum(nullable_value) OVER w AS ns, arraySum(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)) AS ns2,
        count(nullable_value) OVER w AS nc, length(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)) AS nc2,
        min(str) OVER w AS smn, arrayReduce('min', groupArray(str) OVER w) AS smn2,
        max(str) OVER w AS smx, arrayReduce('max', groupArray(str) OVER w) AS smx2
    FROM moving_aggregate_test
    WINDOW w AS (ORDER BY n ROWS BETWEEN 0 PRECEDING AND CURRENT ROW)

    UNION ALL

    SELECT
        1 AS frame_size, n,
        sum(value) OVER w, arraySum(groupArray(value) OVER w),
        count(value) OVER w, length(groupArray(value) OVER w),
        avg(value) OVER w, arrayAvg(groupArray(value) OVER w),
        min(value) OVER w, arrayMin(groupArray(value) OVER w),
        max(value) OVER w, arrayMax(groupArray(value) OVER w),
        sum(nullable_value) OVER w, arraySum(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        count(nullable_value) OVER w, length(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        min(str) OVER w, arrayReduce('min', groupArray(str) OVER w),
        max(str) OVER w, arrayReduce('max', groupArray(str) OVER w)
    FROM moving_aggregate_test
    WINDOW w AS (ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)

    UNION ALL

    SELECT
        2 AS frame_size, n,
        sum(value) OVER w, arraySum(groupArray(value) OVER w),
        count(value) OVER w, length(groupArray(value) OVER w),
        avg(value) OVER w, arrayAvg(groupArray(value) OVER w),
        min(value) OVER w, arrayMin(groupArray(value) OVER w),
        max(value) OVER w, arrayMax(groupArray(value) OVER w),
        sum(nullable_value) OVER w, arraySum(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        count(nullable_value) OVER w, length(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        min(str) OVER w, arrayReduce('min', groupArray(str) OVER w),
        max(str) OVER w, arrayReduce('max', groupArray(str) OVER w)
    FROM moving_aggregate_test
    WINDOW w AS (ORDER BY n ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)

    UNION ALL

    SELECT
        3 AS frame_size, n,
        sum(value) OVER w, arraySum(groupArray(value) OVER w),
        count(value) OVER w, length(groupArray(value) OVER w),
        avg(value) OVER w, arrayAvg(groupArray(value) OVER w),
        min(value) OVER w, arrayMin(groupArray(value) OVER w),
        max(value) OVER w, arrayMax(groupArray(value) OVER w),
        sum(nullable_value) OVER w, arraySum(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        count(nullable_value) OVER w, length(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        min(str) OVER w, arrayReduce('min', groupArray(str) OVER w),
        max(str) OVER w, arrayReduce('max', groupArray(str) OVER w)
    FROM moving_aggregate_test
    WINDOW w AS (ORDER BY n ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)

    UNION ALL

    SELECT
        5 AS frame_size, n,
        sum(value) OVER w, arraySum(groupArray(value) OVER w),
        count(value) OVER w, length(groupArray(value) OVER w),
        avg(value) OVER w, arrayAvg(groupArray(value) OVER w),
        min(value) OVER w, arrayMin(groupArray(value) OVER w),
        max(value) OVER w, arrayMax(groupArray(value) OVER w),
        sum(nullable_value) OVER w, arraySum(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        count(nullable_value) OVER w, length(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        min(str) OVER w, arrayReduce('min', groupArray(str) OVER w),
        max(str) OVER w, arrayReduce('max', groupArray(str) OVER w)
    FROM moving_aggregate_test
    WINDOW w AS (ORDER BY n ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)

    UNION ALL

    SELECT
        10 AS frame_size, n,
        sum(value) OVER w, arraySum(groupArray(value) OVER w),
        count(value) OVER w, length(groupArray(value) OVER w),
        avg(value) OVER w, arrayAvg(groupArray(value) OVER w),
        min(value) OVER w, arrayMin(groupArray(value) OVER w),
        max(value) OVER w, arrayMax(groupArray(value) OVER w),
        sum(nullable_value) OVER w, arraySum(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        count(nullable_value) OVER w, length(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        min(str) OVER w, arrayReduce('min', groupArray(str) OVER w),
        max(str) OVER w, arrayReduce('max', groupArray(str) OVER w)
    FROM moving_aggregate_test
    WINDOW w AS (ORDER BY n ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)

    UNION ALL

    SELECT
        50 AS frame_size, n,
        sum(value) OVER w, arraySum(groupArray(value) OVER w),
        count(value) OVER w, length(groupArray(value) OVER w),
        avg(value) OVER w, arrayAvg(groupArray(value) OVER w),
        min(value) OVER w, arrayMin(groupArray(value) OVER w),
        max(value) OVER w, arrayMax(groupArray(value) OVER w),
        sum(nullable_value) OVER w, arraySum(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        count(nullable_value) OVER w, length(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        min(str) OVER w, arrayReduce('min', groupArray(str) OVER w),
        max(str) OVER w, arrayReduce('max', groupArray(str) OVER w)
    FROM moving_aggregate_test
    WINDOW w AS (ORDER BY n ROWS BETWEEN 50 PRECEDING AND CURRENT ROW)

    UNION ALL

    SELECT
        999 AS frame_size, n,
        sum(value) OVER w, arraySum(groupArray(value) OVER w),
        count(value) OVER w, length(groupArray(value) OVER w),
        avg(value) OVER w, arrayAvg(groupArray(value) OVER w),
        min(value) OVER w, arrayMin(groupArray(value) OVER w),
        max(value) OVER w, arrayMax(groupArray(value) OVER w),
        sum(nullable_value) OVER w, arraySum(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        count(nullable_value) OVER w, length(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        min(str) OVER w, arrayReduce('min', groupArray(str) OVER w),
        max(str) OVER w, arrayReduce('max', groupArray(str) OVER w)
    FROM moving_aggregate_test
    WINDOW w AS (ORDER BY n ROWS BETWEEN 999 PRECEDING AND CURRENT ROW)

    UNION ALL

    SELECT
        10000 AS frame_size, n,
        sum(value) OVER w, arraySum(groupArray(value) OVER w),
        count(value) OVER w, length(groupArray(value) OVER w),
        avg(value) OVER w, arrayAvg(groupArray(value) OVER w),
        min(value) OVER w, arrayMin(groupArray(value) OVER w),
        max(value) OVER w, arrayMax(groupArray(value) OVER w),
        sum(nullable_value) OVER w, arraySum(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        count(nullable_value) OVER w, length(arrayFilter(x -> x IS NOT NULL, groupArray(nullable_value) OVER w)),
        min(str) OVER w, arrayReduce('min', groupArray(str) OVER w),
        max(str) OVER w, arrayReduce('max', groupArray(str) OVER w)
    FROM moving_aggregate_test
    WINDOW w AS (ORDER BY n ROWS BETWEEN 10000 PRECEDING AND CURRENT ROW)
)
GROUP BY frame_size
ORDER BY frame_size;

-- Same check, but with PARTITION BY splitting the input into several partitions, to
-- exercise resetting the moving-aggregate state (and the min/max candidate deque) at
-- partition boundaries.
SELECT
    frame_size,
    countIf(NOT (
        s = s2 AND c = c2 AND (a = a2 OR (isNaN(a) AND isNaN(a2))) AND mn = mn2 AND mx = mx2
    )) AS mismatches
FROM
(
    SELECT
        3 AS frame_size, n,
        sum(value) OVER w AS s, arraySum(groupArray(value) OVER w) AS s2,
        count(value) OVER w AS c, length(groupArray(value) OVER w) AS c2,
        avg(value) OVER w AS a, arrayAvg(groupArray(value) OVER w) AS a2,
        min(value) OVER w AS mn, arrayMin(groupArray(value) OVER w) AS mn2,
        max(value) OVER w AS mx, arrayMax(groupArray(value) OVER w) AS mx2
    FROM moving_aggregate_test
    WINDOW w AS (PARTITION BY part ORDER BY n ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)

    UNION ALL

    SELECT
        10 AS frame_size, n,
        sum(value) OVER w, arraySum(groupArray(value) OVER w),
        count(value) OVER w, length(groupArray(value) OVER w),
        avg(value) OVER w, arrayAvg(groupArray(value) OVER w),
        min(value) OVER w, arrayMin(groupArray(value) OVER w),
        max(value) OVER w, arrayMax(groupArray(value) OVER w)
    FROM moving_aggregate_test
    WINDOW w AS (PARTITION BY part ORDER BY n ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)

    UNION ALL

    SELECT
        50 AS frame_size, n,
        sum(value) OVER w, arraySum(groupArray(value) OVER w),
        count(value) OVER w, length(groupArray(value) OVER w),
        avg(value) OVER w, arrayAvg(groupArray(value) OVER w),
        min(value) OVER w, arrayMin(groupArray(value) OVER w),
        max(value) OVER w, arrayMax(groupArray(value) OVER w)
    FROM moving_aggregate_test
    WINDOW w AS (PARTITION BY part ORDER BY n ROWS BETWEEN 50 PRECEDING AND CURRENT ROW)
)
GROUP BY frame_size
ORDER BY frame_size;

-- RANGE frames: sum/count/avg use the subtract path here, and the sparse ORDER BY key
-- with a bounded frame end produces consecutive frames that can be disjoint, so rows are
-- subtracted before they are added (see the subtract contract in IAggregateFunction.h).
-- avg() over an empty frame is nan while arrayAvg([]) is 0, so skip avg when c = 0.
SELECT
    frame_desc,
    countIf(NOT (s = s2 AND c = c2 AND (c = 0 OR a = a2 OR (isNaN(a) AND isNaN(a2))))) AS mismatches
FROM
(
    SELECT
        'range_100_current' AS frame_desc, n,
        sum(value) OVER w AS s, arraySum(groupArray(value) OVER w) AS s2,
        count(value) OVER w AS c, length(groupArray(value) OVER w) AS c2,
        avg(value) OVER w AS a, arrayAvg(groupArray(value) OVER w) AS a2
    FROM (SELECT *, intDiv(n, 3) * 17 AS sparse_key FROM moving_aggregate_test)
    WINDOW w AS (ORDER BY sparse_key RANGE BETWEEN 100 PRECEDING AND CURRENT ROW)

    UNION ALL

    SELECT
        'range_100_50_preceding' AS frame_desc, n,
        sum(value) OVER w, arraySum(groupArray(value) OVER w),
        count(value) OVER w, length(groupArray(value) OVER w),
        avg(value) OVER w, arrayAvg(groupArray(value) OVER w)
    FROM (SELECT *, intDiv(n, 3) * 17 AS sparse_key FROM moving_aggregate_test)
    WINDOW w AS (ORDER BY sparse_key RANGE BETWEEN 100 PRECEDING AND 50 PRECEDING)
)
GROUP BY frame_desc
ORDER BY frame_desc;

-- Floating point sum/avg must not use the subtract path: a transient Inf/NaN inside the
-- frame must not poison the results of later frames after the offending row leaves.
SELECT
    frame_size,
    countIf(NOT (
        (s = s2 OR (isNaN(s) AND isNaN(s2))) AND (a = a2 OR (isNaN(a) AND isNaN(a2)))
        AND (mn = mn2 OR (isNaN(mn) AND isNaN(mn2))) AND (mx = mx2 OR (isNaN(mx) AND isNaN(mx2)))
    )) AS mismatches
FROM
(
    SELECT
        50 AS frame_size, n,
        sum(fvalue) OVER w AS s, arraySum(groupArray(fvalue) OVER w) AS s2,
        avg(fvalue) OVER w AS a, arrayAvg(groupArray(fvalue) OVER w) AS a2,
        min(fvalue) OVER w AS mn, arrayMin(groupArray(fvalue) OVER w) AS mn2,
        max(fvalue) OVER w AS mx, arrayMax(groupArray(fvalue) OVER w) AS mx2
    FROM moving_aggregate_test
    WINDOW w AS (ORDER BY n ROWS BETWEEN 50 PRECEDING AND CURRENT ROW)
)
GROUP BY frame_size;

DROP TABLE moving_aggregate_test;
