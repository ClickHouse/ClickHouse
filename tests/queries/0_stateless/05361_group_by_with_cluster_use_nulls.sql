-- `group_by_use_nulls` makes the keys `Nullable` only with `ROLLUP`, `CUBE` or `GROUPING SETS`,
-- and none of them can be combined with `WITH CLUSTER`. So the cluster key keeps its type and
-- takes the same path (string, `UInt64`) as without the setting.

SET allow_experimental_group_by_with_cluster = 1;
SET group_by_use_nulls = 1;

SELECT s, toTypeName(s), count() FROM VALUES('s String', ('a'), ('ab'), ('zzzz')) GROUP BY s WITH CLUSTER 1 ORDER BY s;
SELECT s, toTypeName(s), count() FROM VALUES('s LowCardinality(String)', ('a'), ('ab'), ('zzzz')) GROUP BY s WITH CLUSTER 1 ORDER BY s;
SELECT x, toTypeName(x), count() FROM VALUES('x UInt64', (1), (2), (10)) GROUP BY x WITH CLUSTER 1 ORDER BY x;
