-- Regression: after analysis the tuple cluster key `(x, y)` is expanded into
-- two `GROUP BY` keys with `group_by_cluster_dimensions == 2`. `toASTImpl`
-- used to mark only the first scalar as `WITH CLUSTER`, serializing a 2D
-- query as `GROUP BY x WITH CLUSTER d, y` (effectively 1D on `x`) when the
-- query is rewritten back to SQL (e.g. for distributed execution).

-- 2D round-trip: must serialize back as a tuple with `WITH CLUSTER`.
EXPLAIN SYNTAX SELECT count()
FROM (SELECT toUInt64(number) AS x, toUInt64(number * 2) AS y FROM numbers(4))
GROUP BY (x, y) WITH CLUSTER 1;

-- 1D scalar round-trip: scalar form preserved.
EXPLAIN SYNTAX SELECT count()
FROM (SELECT toUInt64(number) AS x FROM numbers(4))
GROUP BY x WITH CLUSTER 1;

-- Extra key before the tuple: cluster modifier stays on the tuple element.
EXPLAIN SYNTAX SELECT count()
FROM (SELECT toUInt64(1) AS k, toUInt64(2) AS x, toUInt64(3) AS y)
GROUP BY k, (x, y) WITH CLUSTER 1;
