-- `-0.0` and `+0.0` have different `Float64` bit patterns but
-- `abs(-0.0 - 0.0) == 0` is `<= distance` for any non-negative distance,
-- so they must always end up in the same cluster. Upstream `Aggregator`
-- hashes floats by raw bits and keeps them in separate groups; the
-- cluster step must canonicalize zero before bucketing.

-- 1D, `distance == 0`: exact-match path bit-casts after canonicalization.
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT x, count() AS c
    FROM (SELECT -0.0 AS x UNION ALL SELECT 0.0)
    GROUP BY x WITH CLUSTER 0
);

-- 1D, `distance > 0`: regular bucket path — same expectation.
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT x, count() AS c
    FROM (SELECT -0.0 AS x UNION ALL SELECT 0.0)
    GROUP BY x WITH CLUSTER 1
);

-- 2D `distance == 0`: dedup path must collapse `-0.0` / `+0.0` on each axis.
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT x, y, count() AS c
    FROM (
        SELECT -0.0 AS x, -0.0 AS y
        UNION ALL SELECT 0.0, 0.0
        UNION ALL SELECT -0.0, 0.0
    )
    GROUP BY (x, y) WITH CLUSTER 0
);
