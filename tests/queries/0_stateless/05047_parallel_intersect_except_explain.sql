-- Tags: no-random-settings, no-parallel-replicas
-- no-random-settings: the pipeline shape depends on max_threads and on the plan optimizations

SET explain_query_plan_default = 'legacy';

-- { echo }

-- With several threads both inputs are scattered by the whole row and one transform runs per partition.
SET max_threads = 4;
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN PIPELINE SELECT number FROM numbers_mt(1000) INTERSECT ALL SELECT number FROM numbers_mt(500)) WHERE explain LIKE '%IntersectOrExcept %' OR explain LIKE '%ScatterByPartitionTransform%';

-- The output streams are disjoint by all columns, so the final DISTINCT does not merge them.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT number FROM numbers_mt(1000) INTERSECT DISTINCT SELECT number FROM numbers_mt(500)) WHERE explain LIKE '%Skip stream merging%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN PIPELINE SELECT number FROM numbers_mt(1000) EXCEPT DISTINCT SELECT number FROM numbers_mt(500)) WHERE explain LIKE '%DistinctTransform%';

-- DISTINCT size limits are global, so the streams are merged before the final DISTINCT.
SET max_rows_in_distinct = 100000;
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN PIPELINE SELECT number FROM numbers_mt(1000) INTERSECT DISTINCT SELECT number FROM numbers_mt(500)) WHERE explain LIKE '%DistinctTransform%';
SET max_rows_in_distinct = 0;

-- With a single thread the old single-transform pipeline is kept.
SET max_threads = 1;
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN PIPELINE SELECT number FROM numbers_mt(1000) INTERSECT ALL SELECT number FROM numbers_mt(500)) WHERE explain LIKE '%IntersectOrExcept %' OR explain LIKE '%ScatterByPartitionTransform%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN PIPELINE SELECT number FROM numbers_mt(1000) EXCEPT DISTINCT SELECT number FROM numbers_mt(500)) WHERE explain LIKE '%DistinctTransform%';
