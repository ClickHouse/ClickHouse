-- Regression: `mergeAggregateStates` used to pass `nullptr` as `Arena` to
-- `IAggregateFunction::merge`, which is undefined behaviour for aggregate
-- functions that grow their state during `merge` (the `groupArray`-family).

-- 1D: merging buckets across the distance keeps growing the `groupArray` state.
SELECT
    arraySort(groupArray(v)) AS vals
FROM (
    SELECT toUInt64(intDiv(number, 5)) AS k, 'v_' || leftPad(toString(number), 2, '0') AS v
    FROM numbers(20)
)
GROUP BY k WITH CLUSTER 1
ORDER BY length(vals);

-- 2D: same check on the Euclidean clustering path. Four `(x, y)` corners of a
-- 1.0 x 1.0 square — all within distance √2 of the diagonal — collapse into a
-- single cluster, exercising arena reallocation across cells.
SELECT
    arraySort(groupArray(v)) AS vals
FROM (
    SELECT 0.0 AS x, 0.0 AS y, 'p00' AS v
    UNION ALL SELECT 1.0, 0.0, 'p10'
    UNION ALL SELECT 0.0, 1.0, 'p01'
    UNION ALL SELECT 1.0, 1.0, 'p11'
)
GROUP BY (x, y) WITH CLUSTER 2.0
ORDER BY length(vals);

-- String: merging buckets across edit distance grows `groupArray`.
SELECT
    arraySort(groupArray(v)) AS vals
FROM (
    SELECT 'abc' AS s, 'v1' AS v
    UNION ALL SELECT 'abd', 'v2'
    UNION ALL SELECT 'abe', 'v3'
)
GROUP BY s WITH CLUSTER 1
ORDER BY length(vals);
