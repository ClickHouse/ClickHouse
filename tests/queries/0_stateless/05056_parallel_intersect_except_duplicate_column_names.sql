-- Tags: no-random-settings, no-parallel-replicas
-- no-random-settings: the pipeline shape depends on max_threads and on the plan optimizations

DROP TABLE IF EXISTS t_intersect_except_dup;
CREATE TABLE t_intersect_except_dup (id UInt32, a String, b String) ENGINE = Memory;
INSERT INTO t_intersect_except_dup VALUES (1, 'hello', 'world'), (2, 'foo', 'bar');

SET max_threads = 4;

-- The CI test config (users.d/limits.yaml) sets global DISTINCT size limits, which keep the stream
-- merge before the final DISTINCT; reset them so only the duplicate names decide.
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;

SET explain_query_plan_default = 'legacy';

-- { echo }

-- `SELECT id, *, b` repeats `id` and `b` in the output header. The partitioned pipeline has to stay
-- correct for such a header. The set operations stay top-level: the old analyzer deduplicates the
-- names once the query is wrapped in `SELECT * FROM (...)`. Every result is a single row.
(SELECT id, *, b FROM t_intersect_except_dup) EXCEPT DISTINCT (SELECT id, *, b FROM t_intersect_except_dup WHERE id = 1);
(SELECT id, *, b FROM t_intersect_except_dup) INTERSECT DISTINCT (SELECT id, *, b FROM t_intersect_except_dup WHERE id = 1);
(SELECT id, *, b FROM t_intersect_except_dup) EXCEPT ALL (SELECT id, *, b FROM t_intersect_except_dup WHERE id = 1);
(SELECT id, *, b FROM t_intersect_except_dup) INTERSECT ALL (SELECT id, *, b FROM t_intersect_except_dup WHERE id = 1);

-- Both inputs are still scattered, so one transform runs per partition.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN PIPELINE (SELECT id, *, b FROM t_intersect_except_dup) INTERSECT DISTINCT (SELECT id, *, b FROM t_intersect_except_dup)) WHERE explain LIKE '%IntersectOrExcept %';

-- The scatter partitions by column position, while the disjointness property is matched by column
-- name downstream. A duplicate-name header cannot express that partitioning, so the property is
-- dropped and the final DISTINCT keeps merging the streams into one.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 (SELECT id, *, b FROM t_intersect_except_dup) INTERSECT DISTINCT (SELECT id, *, b FROM t_intersect_except_dup)) WHERE explain LIKE '%Distinct%' OR explain LIKE '%Skip stream merging%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN PIPELINE (SELECT id, *, b FROM t_intersect_except_dup) INTERSECT DISTINCT (SELECT id, *, b FROM t_intersect_except_dup)) WHERE explain LIKE '%DistinctTransform%';

-- The same query with a header whose names are unique does keep the property.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 (SELECT id, a, b FROM t_intersect_except_dup) INTERSECT DISTINCT (SELECT id, a, b FROM t_intersect_except_dup)) WHERE explain LIKE '%Distinct%' OR explain LIKE '%Skip stream merging%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN PIPELINE (SELECT id, a, b FROM t_intersect_except_dup) INTERSECT DISTINCT (SELECT id, a, b FROM t_intersect_except_dup)) WHERE explain LIKE '%DistinctTransform%';

-- { echoOff }

DROP TABLE t_intersect_except_dup;
