-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: Explain output may differ

-- The optimizations below are disabled under parallel replicas.
SET enable_parallel_replicas = 0;

-- Some CI configurations set DISTINCT and GROUP BY limits at the server level; pin them to unlimited
-- so that the independent per-partition DISTINCT below is applied and creates disjoint streams, and
-- only the per-query SETTINGS control the GROUP BY limit.
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

SET max_threads = 8;
SET allow_distinct_partitions_independently = 1;
SET force_distinct_partitions_independently = 1;
SET allow_aggregate_partitions_independently = 1;

-- The pretty EXPLAIN output decorates plan lines with tree-drawing characters; use the legacy format
-- so the assertions below match plain `Skip merging: 1` lines.
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_disjoint_group_limit;
CREATE TABLE t_disjoint_group_limit (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO t_disjoint_group_limit SELECT number, number FROM numbers(800);

-- An inner independent per-partition DISTINCT keeps the partition streams disjoint, and the disjointness
-- propagates to the outer GROUP BY, which then skips merging its per-stream hash tables.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN actions = 1 SELECT a, count() FROM (SELECT DISTINCT a, b FROM t_disjoint_group_limit) GROUP BY a) WHERE explain LIKE '%Skip merging: 1%';

-- `max_rows_to_group_by` is enforced during the merge phase, so with a nonzero limit the GROUP BY must
-- keep the merge (no `Skip merging: 1` in the plan) and the limit stays global: 800 groups exceed 200
-- and the query fails, exactly as without the optimizations.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN actions = 1 SELECT a, count() FROM (SELECT DISTINCT a, b FROM t_disjoint_group_limit) GROUP BY a SETTINGS max_rows_to_group_by = 200, group_by_overflow_mode = 'throw') WHERE explain LIKE '%Skip merging: 1%';
SELECT a, count() FROM (SELECT DISTINCT a, b FROM t_disjoint_group_limit) GROUP BY a SETTINGS max_rows_to_group_by = 200, group_by_overflow_mode = 'throw' FORMAT Null; -- { serverError TOO_MANY_ROWS }
SELECT a, count() FROM (SELECT DISTINCT a, b FROM t_disjoint_group_limit) GROUP BY a SETTINGS allow_distinct_partitions_independently = 0, allow_aggregate_partitions_independently = 0, max_rows_to_group_by = 200, group_by_overflow_mode = 'throw' FORMAT Null; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_disjoint_group_limit;
