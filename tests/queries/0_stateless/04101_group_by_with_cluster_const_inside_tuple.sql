-- Regression: the constant-elimination branch in
-- `analyzeAggregation` used to throw `BAD_ARGUMENTS` when a constant key
-- happened to land inside the clustered key range — e.g.
-- `GROUP BY (x, 0) WITH CLUSTER d` in an aggregate query — even though
-- the 2D tuple validation accepts the shape. Keep such constants in the
-- aggregation stream so the cluster contract stays consistent across
-- aggregate / no-aggregate query shapes.

-- Aggregate query (count()): used to fail before the fix.
SELECT count() AS num_clusters
FROM (
    SELECT k, count() AS c
    FROM (SELECT toUInt64(number) AS k FROM numbers(4))
    GROUP BY (k, 0) WITH CLUSTER 1
);

-- Same shape, no aggregate (just identity): always worked, must keep working.
SELECT k
FROM (SELECT toUInt64(number) AS k FROM numbers(4))
GROUP BY (k, 0) WITH CLUSTER 1
ORDER BY k;
