-- Regression: `Phase A` buckets 1D rows by `floor(key / distance)` and 2D rows by
-- `floor(coord / (distance / sqrt(2)))`, then merges same-bucket/cell rows without a
-- distance check. That is only safe while the quotient fits Float64's exact-integer
-- range; past `2^53` its ULP exceeds 1, so keys farther apart than the distance can
-- round to the same bucket and be merged. Such inputs must be rejected, not silently
-- misclustered.

SET enable_analyzer = 1; -- `WITH CLUSTER` is implemented for the analyzer only
SET allow_experimental_group_by_with_cluster = 1;

-- 1D: `10.0 / 1e-15 = 1e16 > 2^53`, so the bucket id is imprecise -> reject.
SELECT 'reject 1D imprecise bucket';
SELECT count() FROM (
    SELECT count() FROM VALUES('x Float64', (10.0), (10.000000000000002))
    GROUP BY x WITH CLUSTER 1e-15
); -- { serverError BAD_ARGUMENTS }

-- 2D: the same for the grid cell index at a large coordinate / tiny distance.
SELECT 'reject 2D imprecise cell';
SELECT count() FROM (
    SELECT count() FROM VALUES('x Float64, y Float64', (707106.78, 0.0), (707106.79, 0.0))
    GROUP BY (x, y) WITH CLUSTER 1e-10
); -- { serverError BAD_ARGUMENTS }

-- Reasonable magnitudes must still cluster normally (no over-rejection).
SELECT '1D still works';
SELECT count() AS num_clusters FROM (
    SELECT count() FROM VALUES('x Float64', (1.0), (1.5), (100.0))
    GROUP BY x WITH CLUSTER 1.0
);

SELECT '2D still works';
SELECT count() AS num_clusters FROM (
    SELECT count() FROM VALUES('x Float64, y Float64', (0.0, 0.0), (0.5, 0.0), (100.0, 100.0))
    GROUP BY (x, y) WITH CLUSTER 1.0
);

-- Large values with a large distance keep a small ratio and must not be rejected.
SELECT '1D large values large distance still works';
SELECT count() AS num_clusters FROM (
    SELECT count() FROM VALUES('x Float64', (0.0), (2e300))
    GROUP BY x WITH CLUSTER 1e300
);
