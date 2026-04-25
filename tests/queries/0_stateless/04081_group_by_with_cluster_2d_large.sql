-- Larger-scale and cell-boundary tests for 2D `GROUP BY ... WITH CLUSTER`.
-- Cell side `a = d / sqrt(2)`, so the grid algorithm probes a 5x5 neighborhood.
-- These tests exercise that neighborhood at its corners and verify bulk
-- correctness on tens of thousands of rows.

SELECT '--- Cell-boundary: distance exactly d (3-4-5 triangle) ---';
-- (0, 0) and (3, 4): distance 5 exactly (no FP rounding). At d = 5 the check
-- `dist_sq <= d_sq` (25 <= 25) merges; at d = 4 it does not.
-- a = 5 / sqrt(2) ~= 3.535; cells: (0, 0) and (0, 1) -> forward neighbor.
SELECT count() AS num_groups
FROM (
    SELECT (x, y) AS p
    FROM VALUES('x Float64, y Float64', (0.0, 0.0), (3.0, 4.0))
    GROUP BY (x, y) WITH CLUSTER 5
);

SELECT '--- Cell-boundary: same points, d = 4 -> no merge ---';
SELECT count() AS num_groups
FROM (
    SELECT (x, y) AS p
    FROM VALUES('x Float64, y Float64', (0.0, 0.0), (3.0, 4.0))
    GROUP BY (x, y) WITH CLUSTER 4
);

SELECT '--- Reach across two cells horizontally ---';
-- a ~= 7.07; (0, 0) in cell (0, 0); (9.9, 0) in cell (1, 0). Distance 9.9 < 10 -> merge.
SELECT count() AS num_groups
FROM (
    SELECT (x, y) AS p
    FROM VALUES('x Float64, y Float64', (0.0, 0.0), (9.9, 0.0))
    GROUP BY (x, y) WITH CLUSTER 10
);

SELECT '--- Reach across two cells horizontally, just out of range ---';
SELECT count() AS num_groups
FROM (
    SELECT (x, y) AS p
    FROM VALUES('x Float64, y Float64', (0.0, 0.0), (10.01, 0.0))
    GROUP BY (x, y) WITH CLUSTER 10
);

SELECT '--- 5x5 corner: (2, 2) cell offset, points placed inside d ---';
-- Cells (0, 0) and (2, 2). Points at (a - 0.5, a - 0.5) and (2a + 0.5, 2a + 0.5)
-- in those cells (a ~= 7.07). The closest pair is across the cell-block diagonal
-- with distance ~ a + 1, which can be > d. Use small jitter to keep within d.
-- (a - 0.5) -> in cell (0, 0); (a + 0.5) -> in cell (1, 1)? floor(7.57 / 7.07) = 1.
-- Pick coordinates that land in (0, 0) and (2, 2) and are within d of each other.
-- (1, 1) -> cell (0, 0). (2 * a + 1, 2 * a + 1) ~= (15.14, 15.14) -> cell (2, 2).
-- Distance ~ sqrt(2) * (2 * a) ~= 20 > 10. Not within d -> separate clusters.
SELECT count() AS num_groups
FROM (
    SELECT (x, y) AS p
    FROM VALUES('x Float64, y Float64', (1.0, 1.0), (2.0 * sqrt(50.0) + 1.0, 2.0 * sqrt(50.0) + 1.0))
    GROUP BY (x, y) WITH CLUSTER 10
);

SELECT '--- 100 well-separated clusters x 100 distinct points (10K rows) ---';
-- Centers on a 10 x 10 grid spaced by 1000. Each cluster: 10 x 10 grid of
-- distinct integer points spread within the unit square [0, 9]^2 around the
-- center. Within-cluster max pair distance = sqrt(162) ~= 12.7 < d = 20.
-- Between-cluster min distance = 1000 - 9 - 9 = 982 >> d.
-- Expected: 100 groups, each of size 100, total 10000.
SELECT
    count() AS num_groups,
    sum(c) AS total_points,
    min(c) AS min_size,
    max(c) AS max_size
FROM (
    SELECT (x, y) AS p, count() AS c
    FROM (
        SELECT
            (intDiv(number, 100) % 10) * 1000.0 + toFloat64(number % 10) AS x,
            intDiv(intDiv(number, 100), 10) * 1000.0 + toFloat64(intDiv(number % 100, 10)) AS y
        FROM numbers(10000)
    )
    GROUP BY (x, y) WITH CLUSTER 20
);

SELECT '--- Long chain along x-axis: 1000 points, all merge ---';
SELECT count() AS num_groups, sum(c) AS total_points
FROM (
    SELECT (x, y) AS p, count() AS c
    FROM (SELECT toFloat64(number) AS x, 0.0 AS y FROM numbers(1000))
    GROUP BY (x, y) WITH CLUSTER 1.5
);

SELECT '--- Long chain along x-axis: same data, d below step -> no merge ---';
SELECT count() AS num_groups, sum(c) AS total_points
FROM (
    SELECT (x, y) AS p, count() AS c
    FROM (SELECT toFloat64(number) AS x, 0.0 AS y FROM numbers(1000))
    GROUP BY (x, y) WITH CLUSTER 0.5
);

SELECT '--- Diagonal chain: 500 points along y = x, step 1, d ~ 1.5 ---';
-- Consecutive distance = sqrt(2) ~= 1.414 < 1.5 -> all merge.
SELECT count() AS num_groups, sum(c) AS total_points
FROM (
    SELECT (x, y) AS p, count() AS c
    FROM (SELECT toFloat64(number) AS x, toFloat64(number) AS y FROM numbers(500))
    GROUP BY (x, y) WITH CLUSTER 1.5
);

SELECT '--- Many tiny clusters partitioned by non-cluster key ---';
-- 50 categories, each with 200 distinct x-positions far apart on x-axis.
-- d = 1, points spaced by 100 -> no merging within a category, and the
-- non-cluster key already separates categories.
-- Expected: 50 * 200 = 10000 groups.
SELECT count() AS num_groups, sum(c) AS total_points
FROM (
    SELECT cat, (x, y) AS p, count() AS c
    FROM (
        SELECT
            number % 50 AS cat,
            toFloat64(intDiv(number, 50) % 200) * 100.0 AS x,
            0.0 AS y
        FROM numbers(50000)
    )
    GROUP BY cat, (x, y) WITH CLUSTER 1
);

SELECT '--- Same coordinates per non-cluster key, no cross-key merge ---';
-- All categories have points at (0, 0), (1, 0), (2, 0). d = 5 merges within
-- each category, but the non-cluster key prevents merging across categories.
-- Expected: 100 categories x 1 cluster = 100 groups, total = 100 * 3 = 300.
SELECT count() AS num_groups, sum(c) AS total_points
FROM (
    SELECT cat, (x, y) AS p, count() AS c
    FROM (
        SELECT
            intDiv(number, 3) AS cat,
            toFloat64(number % 3) AS x,
            0.0 AS y
        FROM numbers(300)
    )
    GROUP BY cat, (x, y) WITH CLUSTER 5
);

SELECT '--- Two interleaved chains separated by gap > d ---';
-- Even-indexed points form a chain on y = 0; odd-indexed form a chain on y = 100.
-- Both chains are dense (step = 1), but the y-gap (100) exceeds d = 5.
-- Expected: 2 groups, total = 1000.
SELECT count() AS num_groups, sum(c) AS total_points, min(c) AS min_size, max(c) AS max_size
FROM (
    SELECT (x, y) AS p, count() AS c
    FROM (
        SELECT
            toFloat64(intDiv(number, 2)) AS x,
            if(number % 2 = 0, 0.0, 100.0) AS y
        FROM numbers(1000)
    )
    GROUP BY (x, y) WITH CLUSTER 5
);

SELECT '--- Dense single cluster: 10K points inside one cell ---';
-- Cell side a = d / sqrt(2); for d = 100, a ~= 70.7. Place 10K distinct points
-- in [0, 50] x [0, 50] -- all in cell (0, 0). Phase A merges them in O(n);
-- Phase B has nothing to do. Expected: 1 group of size 10000.
SELECT count() AS num_groups, sum(c) AS total_points
FROM (
    SELECT (x, y) AS p, count() AS c
    FROM (
        SELECT
            toFloat64(number % 100) * 0.5 AS x,
            toFloat64(intDiv(number, 100) % 100) * 0.5 AS y
        FROM numbers(10000)
    )
    GROUP BY (x, y) WITH CLUSTER 100
);

SELECT '--- Negative coordinates, mixed quadrants ---';
-- Four clusters, one in each quadrant, separated by 1000 in both axes.
SELECT count() AS num_groups, sum(c) AS total_points
FROM (
    SELECT (x, y) AS p, count() AS c
    FROM (
        SELECT
            (if(number % 4 < 2, -500.0, 500.0)) + toFloat64(intDiv(number, 4) % 10) AS x,
            (if(number % 2 = 0, -500.0, 500.0)) + toFloat64(intDiv(intDiv(number, 4), 10) % 10) AS y
        FROM numbers(4000)
    )
    GROUP BY (x, y) WITH CLUSTER 50
);

SELECT '--- Multiple aggregate functions over a large dataset ---';
SELECT
    count() AS num_groups,
    sum(s) AS total_sum,
    sum(c) AS total_count
FROM (
    SELECT
        (x, y) AS p,
        count() AS c,
        sum(v) AS s
    FROM (
        SELECT
            (intDiv(number, 50) % 20) * 1000.0 + toFloat64(number % 5) AS x,
            intDiv(intDiv(number, 50), 20) * 1000.0 + toFloat64(intDiv(number % 50, 5)) AS y,
            toInt32(number) AS v
        FROM numbers(20000)
    )
    GROUP BY (x, y) WITH CLUSTER 30
);
