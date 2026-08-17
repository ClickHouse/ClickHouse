-- `ReadFromMerge` applies `_table` / `_database` pruning before it plans the
-- children. The two local views become a single `ReadFromMergeTree` read here,
-- so the normal planner can silently disable parallel replicas by the min-rows
-- estimate even in forcing mode. The recursive-step preflight must not reject
-- the unpruned pair of views before the filter selects one child.
CREATE TABLE edges (from_id UInt64, to_id UInt64) ENGINE = MergeTree ORDER BY from_id;
INSERT INTO edges SELECT number, number + 1 FROM numbers(10);

CREATE VIEW edges_view_v1 AS SELECT * FROM edges;
CREATE VIEW edges_view_v2 AS SELECT * FROM edges;
CREATE TABLE edges_merge AS edges ENGINE = Merge(currentDatabase(), '^edges_view_v[12]$');

WITH RECURSIVE walk AS
(
    SELECT 1 AS n
  UNION ALL
    SELECT n + 1 FROM walk AS t INNER JOIN edges_merge AS e ON e.from_id = t.n
    WHERE n < 10 AND e._table = 'edges_view_v1'
)
SELECT sum(n) FROM walk
SETTINGS allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_allow_view_over_mergetree = 0,
    automatic_parallel_replicas_mode = 0, parallel_replicas_min_number_of_rows_per_replica = 1000000;

DROP TABLE edges_merge;
DROP VIEW edges_view_v2;
DROP VIEW edges_view_v1;
DROP TABLE edges;
