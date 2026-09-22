-- Regression: the 2D Euclidean distance check must not overflow for valid finite
-- `Float64` coordinates. With a large distance the squared form `dx*dx + dy*dy`
-- (and the cell AABB `gap*gap`) overflow to `+inf`, and `inf <= inf` used to merge
-- points farther apart than the requested distance. The overflow-safe check
-- (per-axis reject + `std::hypot`) keeps such points separate.

SET enable_analyzer = 1; -- `WITH CLUSTER` is implemented for the analyzer only
SET allow_experimental_group_by_with_cluster = 1;

-- Far apart in different cells: the real distance 2e300 > 1e300, so the squared
-- form overflowed and merged them; they must stay as 2 clusters.
SELECT count() AS num_clusters FROM (
    SELECT count() FROM VALUES('x Float64, y Float64', (0.0, 0.0), (2e300, 0.0))
    GROUP BY (x, y) WITH CLUSTER 1e300
);

-- Within the distance but in different cells: 8e299 < 1e300, must merge to 1 cluster
-- (the overflow-safe check must not over-reject finite points either).
SELECT count() AS num_clusters FROM (
    SELECT count() FROM VALUES('x Float64, y Float64', (0.0, 0.0), (8e299, 0.0))
    GROUP BY (x, y) WITH CLUSTER 1e300
);
