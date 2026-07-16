-- Tags: no-fasttest

-- Regression test for the sliding window frame aggregation in
-- WindowTransform::updateAggregationState(): when the frame start advances, the
-- aggregate state is rebuilt from a FlatFAT-style tree of partial aggregate states
-- (FrameAggregateTree) once the frame is observed to have at least 2048 rows, and by
-- plain reset-and-readd below that. Frame sizes here cover both paths and the crossover.
-- Aggregates are cross-checked against array functions over groupArray(...) OVER the
-- same frame; groupArray itself goes through the tree too (merge concatenates in frame
-- order), and is checked against ground truth in a separate section below.

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
    number % 2,
    (cityHash64(number) % 201) - 100,
    if(number % 11 = 0, NULL, (cityHash64(number, 1) % 201) - 100),
    toString(cityHash64(number, 2) % 5),
    -- Division by 8 keeps the values exactly representable, so float sums are
    -- order-independent and comparable exactly.
    multiIf(number = 500, inf, number = 600, -inf, number = 700, nan, (toInt64(cityHash64(number, 3) % 201) - 100) / 8)
FROM numbers(6000);

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
        100 AS frame_size, n,
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
    WINDOW w AS (ORDER BY n ROWS BETWEEN 100 PRECEDING AND CURRENT ROW)

    UNION ALL

    SELECT
        2500 AS frame_size, n,
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
    WINDOW w AS (ORDER BY n ROWS BETWEEN 2500 PRECEDING AND CURRENT ROW)

    UNION ALL

    SELECT
        3000 AS frame_size, n,
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
    WINDOW w AS (ORDER BY n ROWS BETWEEN 3000 PRECEDING AND CURRENT ROW)
)
GROUP BY frame_size
ORDER BY frame_size;

-- Same check, but with PARTITION BY splitting the input into two partitions of 3000
-- rows, to exercise resetting the frame aggregate tree at partition boundaries.
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

    UNION ALL

    SELECT
        2500 AS frame_size, n,
        sum(value) OVER w, arraySum(groupArray(value) OVER w),
        count(value) OVER w, length(groupArray(value) OVER w),
        avg(value) OVER w, arrayAvg(groupArray(value) OVER w),
        min(value) OVER w, arrayMin(groupArray(value) OVER w),
        max(value) OVER w, arrayMax(groupArray(value) OVER w)
    FROM moving_aggregate_test
    WINDOW w AS (PARTITION BY part ORDER BY n ROWS BETWEEN 2500 PRECEDING AND CURRENT ROW)
)
GROUP BY frame_size
ORDER BY frame_size;

-- RANGE frames: the sparse ORDER BY key with a bounded frame end produces consecutive
-- frames that can be entirely disjoint, so the frame start may jump past the previous
-- frame end.
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

-- RANGE frames big enough for the tree, with a dense key so every peer group has
-- several rows (consecutive rows then share the same frame), and with a key that jumps
-- periodically so that consecutive frames are sometimes disjoint.
SELECT
    frame_desc,
    countIf(NOT (s = s2 AND c = c2 AND mn = mn2 AND u = u2)) AS mismatches
FROM
(
    SELECT
        'range_dense' AS frame_desc, n,
        sum(value) OVER w AS s, arraySum(groupArray(value) OVER w) AS s2,
        count(value) OVER w AS c, length(groupArray(value) OVER w) AS c2,
        min(value) OVER w AS mn, arrayMin(groupArray(value) OVER w) AS mn2,
        uniqExact(str) OVER w AS u, length(arrayDistinct(groupArray(str) OVER w)) AS u2
    FROM (SELECT *, intDiv(n, 100) AS range_key FROM moving_aggregate_test)
    WINDOW w AS (ORDER BY range_key RANGE BETWEEN 25 PRECEDING AND CURRENT ROW)

    UNION ALL

    SELECT
        'range_jumping' AS frame_desc, n,
        sum(value) OVER w, arraySum(groupArray(value) OVER w),
        count(value) OVER w, length(groupArray(value) OVER w),
        min(value) OVER w, arrayMin(groupArray(value) OVER w),
        uniqExact(str) OVER w, length(arrayDistinct(groupArray(str) OVER w))
    FROM (SELECT *, n + intDiv(n, 3000) * 8000 AS range_key FROM moving_aggregate_test)
    WINDOW w AS (ORDER BY range_key RANGE BETWEEN 2500 PRECEDING AND CURRENT ROW)
)
GROUP BY frame_desc
ORDER BY frame_desc;

-- Floating point: a transient Inf/NaN inside the frame must not poison the results of
-- later frames after the offending row leaves. The 50-row frame stays on the recompute
-- path, the 2500-row frame goes through the tree (the segments containing the Inf/NaN
-- rows simply stop being merged once the frame has passed them).
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

    UNION ALL

    SELECT
        2500 AS frame_size, n,
        sum(fvalue) OVER w, arraySum(groupArray(fvalue) OVER w),
        avg(fvalue) OVER w, arrayAvg(groupArray(fvalue) OVER w),
        min(fvalue) OVER w, arrayMin(groupArray(fvalue) OVER w),
        max(fvalue) OVER w, arrayMax(groupArray(fvalue) OVER w)
    FROM moving_aggregate_test
    WINDOW w AS (ORDER BY n ROWS BETWEEN 2500 PRECEDING AND CURRENT ROW)
)
GROUP BY frame_size
ORDER BY frame_size;

-- groupArray through the tree against ground truth: merge must concatenate the segments
-- in frame order, so the result must be exactly the frame rows in order.
SELECT countIf(arrayMap(x -> toUInt64(x), ga) != range(if(n < 2500, toUInt64(0), toUInt64(n - 2500)), toUInt64(n) + 1)) AS mismatches
FROM
(
    SELECT
        n,
        groupArray(n) OVER (ORDER BY n ROWS BETWEEN 2500 PRECEDING AND CURRENT ROW) AS ga
    FROM moving_aggregate_test
);

-- Functions with non-trivial states and combinators through the tree, cross-checked
-- against array functions over groupArray (itself verified above).
SELECT countIf(NOT (u = u2 AND si = si2 AND q = q2)) AS mismatches
FROM
(
    SELECT
        n,
        uniqExact(value) OVER w AS u, length(arrayDistinct(groupArray(value) OVER w)) AS u2,
        sumIf(value, value % 2 = 0) OVER w AS si, arraySum(arrayFilter(x -> x % 2 = 0, groupArray(value) OVER w)) AS si2,
        quantileExact(value) OVER w AS q, arrayReduce('quantileExact', groupArray(value) OVER w) AS q2
    FROM moving_aggregate_test
    WINDOW w AS (ORDER BY n ROWS BETWEEN 2500 PRECEDING AND CURRENT ROW)
);

-- Extreme signed values: the tree never negates values (rows leaving the frame simply
-- stop being merged, there is no inverse transition), so the minimum Int64 value passing
-- through a sliding frame must produce exact results, including after it leaves the
-- frame. avg is checked only after the extreme row has left, because its Float64
-- division is not exactly comparable against arrayAvg while the huge value is in frame.
SELECT countIf(NOT (s = s2 AND mn = mn2)) + countIf(n > 2600 AND (s != 2501 OR mn != 1 OR a != 1)) AS mismatches
FROM
(
    SELECT
        n,
        sum(ev) OVER w AS s, arraySum(groupArray(ev) OVER w) AS s2,
        min(ev) OVER w AS mn, arrayMin(groupArray(ev) OVER w) AS mn2,
        avg(ev) OVER w AS a
    FROM (SELECT n, if(n = 100, toInt64(-9223372036854775808), toInt64(1)) AS ev FROM moving_aggregate_test)
    WINDOW w AS (ORDER BY n ROWS BETWEEN 2500 PRECEDING AND CURRENT ROW)
);

-- The frame shrinks exactly when a higher-level segment group of the tree completes
-- (the key jump is aligned so the skipped group is the last child of a built level-2
-- group), then regrows past two level-2 spans. Regression for a parent segment being
-- built into the wrong slot after such a skip.
SELECT countIf(c != cnt OR s != expected_s) AS mismatches
FROM
(
    SELECT
        n,
        if(n < 16200, greatest(toInt64(n) - 12000, 0), greatest(toInt64(16200), toInt64(n) - 12000)) AS lo,
        toInt64(n) - lo + 1 AS cnt,
        intDiv((lo + toInt64(n)) * cnt, 2) AS expected_s,
        count() OVER w AS c, sum(n) OVER w AS s
    FROM (SELECT number AS n, if(number < 16200, number, number + 13000) AS k FROM numbers(30000))
    WINDOW w AS (ORDER BY k RANGE BETWEEN 12000 PRECEDING AND CURRENT ROW)
);

-- Functions without mergeIsEquivalentToAddingRows must keep the recompute path: their
-- results must match a sequential re-aggregation (arrayReduce), also through
-- combinators. The cr check skips the growing prefix, where the tail-add path reuses
-- the state after insertResultInto finalized the compressor (pre-existing behavior).
SELECT countIf(NOT (gs = gs2 AND (q = q2 OR (isNaN(q) AND isNaN(q2))) AND tk = tk2 AND (qi = qi2 OR (isNaN(qi) AND isNaN(qi2))) AND (n <= 2500 OR cr = cr2) AND gu = gu2 AND guu = guu2)) AS mismatches
FROM
(
    SELECT
        n,
        groupArraySample(2, 1)(value) OVER w AS gs, arrayReduce('groupArraySample(2, 1)', groupArray(value) OVER w) AS gs2,
        quantile(0.5)(value) OVER w AS q, arrayReduce('quantile(0.5)', groupArray(value) OVER w) AS q2,
        topK(3)(value) OVER w AS tk, arrayReduce('topK(3)', groupArray(value) OVER w) AS tk2,
        quantileIf(0.5)(value, value % 2 = 0) OVER w AS qi, arrayReduce('quantileIf(0.5)', groupArray(value) OVER w, groupArray(value % 2 = 0) OVER w) AS qi2,
        estimateCompressionRatio('ZSTD')(str) OVER w AS cr, arrayReduce('estimateCompressionRatio(\'ZSTD\')', groupArray(str) OVER w) AS cr2,
        groupUniqArray(2)(value) OVER w AS gu, arrayReduce('groupUniqArray(2)', groupArray(value) OVER w) AS gu2,
        groupUniqArray(value) OVER w AS guu, arrayReduce('groupUniqArray', groupArray(value) OVER w) AS guu2
    FROM moving_aggregate_test
    WINDOW w AS (ORDER BY n ROWS BETWEEN 2500 PRECEDING AND CURRENT ROW)
);

-- groupArrayIntersect must stay on the recompute path: the result array exposes
-- hash-table order, and the rotated core makes per-row insertion order differ.
SELECT countIf(gi != gi2) AS mismatches
FROM
(
    SELECT
        groupArrayIntersect(arr) OVER w AS gi, arrayReduce('groupArrayIntersect', groupArray(arr) OVER w) AS gi2
    FROM (SELECT n, arrayRotateLeft(arrayMap(x -> cityHash64(x) % 100000, range(40)), toInt32(n % 40)) AS arr FROM moving_aggregate_test)
    WINDOW w AS (ORDER BY n ROWS BETWEEN 2500 PRECEDING AND CURRENT ROW)
);

-- Zero-sized aggregate states (the Nothing placeholders for only-NULL arguments) must
-- stay on the recompute path.
SELECT countIf(c != 0 OR s IS NOT NULL) AS mismatches
FROM
(
    SELECT count(NULL) OVER w AS c, sum(toNullable(NULL)) OVER w AS s
    FROM moving_aggregate_test
    WINDOW w AS (ORDER BY n ROWS BETWEEN 3000 PRECEDING AND CURRENT ROW)
);

-- Float rounding over tree-sized frames may differ from sequential summation (which
-- already depends on the block layout, on master too). Pin what is guaranteed:
-- identical runs give identical bits, and the frame row multiset stays exact.
DROP TABLE IF EXISTS float_order_results;
CREATE TABLE float_order_results (r UInt64, c UInt64) ENGINE = Memory;
INSERT INTO float_order_results
SELECT groupBitXor(reinterpretAsUInt64(s)), countIf(cnt != least(n, 2999) + 1)
FROM (SELECT number AS n, count() OVER w AS cnt, sum(multiIf(number % 3 = 0, 1e16, number % 3 = 1, -1e16, 1.)) OVER w AS s FROM numbers(20000)
      WINDOW w AS (ORDER BY number ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW))
SETTINGS max_block_size = 123;
INSERT INTO float_order_results
SELECT groupBitXor(reinterpretAsUInt64(s)), countIf(cnt != least(n, 2999) + 1)
FROM (SELECT number AS n, count() OVER w AS cnt, sum(multiIf(number % 3 = 0, 1e16, number % 3 = 1, -1e16, 1.)) OVER w AS s FROM numbers(20000)
      WINDOW w AS (ORDER BY number ROWS BETWEEN 2999 PRECEDING AND CURRENT ROW))
SETTINGS max_block_size = 123;
SELECT uniqExact(r), max(c) FROM float_order_results;
DROP TABLE float_order_results;

DROP TABLE moving_aggregate_test;
