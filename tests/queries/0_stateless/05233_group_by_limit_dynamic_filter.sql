-- Tags: no-parallel-replicas
-- no-parallel-replicas: the top-K heap needs `LimitStep` directly above `AggregatingStep` in a
-- single-stage plan, and the dynamic filter links the aggregation to the local reading step.

-- `GROUP BY key [ORDER BY key] LIMIT n` publishes the boundary of the aggregation's top-K heap to
-- the `MergeTree` reading step (`enable_group_by_top_k_dynamic_filtering`): rows whose key lies
-- beyond the boundary are dropped by a PREWHERE before the other columns are read, and granules
-- that lie entirely beyond it are skipped through the primary index.

-- Neither the heap nor its filter applies to serialized plans; pin the setting so the assertions
-- hold in the distributed-plan suite.
SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;

-- Settings the CI randomizes that would disable the heap (`max_rows_to_group_by`, a tiny top-K
-- limit, aggregation in order) or change what the assertions observe (read-time granule
-- skipping, the block size that paces the publication of the boundary, threads).
SET max_rows_to_group_by = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
SET enable_group_by_top_k_dynamic_filtering = 1;
SET use_top_k_dynamic_filtering = 1;
SET use_top_k_dynamic_filtering_for_variable_length_types = 0;
SET optimize_aggregation_in_order = 0;
SET optimize_trivial_group_by_limit_query = 0;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;
SET max_threads = 1;
SET max_block_size = 1024;
SET log_queries = 1;

DROP TABLE IF EXISTS t_gb_dyn;
DROP TABLE IF EXISTS ref_a;
DROP TABLE IF EXISTS ref_b;
DROP TABLE IF EXISTS ref_ab;
DROP TABLE IF EXISTS ref_n;

CREATE TABLE t_gb_dyn (a UInt32, b UInt32, s String, n Nullable(UInt32))
ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 128;

-- One part: 100 values of `a` with 1000 rows each, so a value of `a` spans about 8 granules,
-- and `b` restarts from 0 inside every `a`.
SET max_insert_threads = 1;
SET min_insert_block_size_rows = 1000000;
INSERT INTO t_gb_dyn
SELECT intDiv(number, 1000), number % 1000, toString(number), if(number % 7 = 0, NULL, number % 300)
FROM numbers(100000);

CREATE TABLE ref_a ENGINE = Memory AS SELECT a, count() AS c, sum(b) AS sb FROM t_gb_dyn GROUP BY a;
CREATE TABLE ref_b ENGINE = Memory AS SELECT b, count() AS c, sum(a) AS sa FROM t_gb_dyn GROUP BY b;
CREATE TABLE ref_ab ENGINE = Memory AS SELECT a, intDiv(b, 100) AS bb, count() AS c FROM t_gb_dyn GROUP BY a, bb;
CREATE TABLE ref_n ENGINE = Memory AS SELECT n, count() AS c FROM t_gb_dyn GROUP BY n;

-- Every returned group must carry its complete aggregates.

SELECT 'first primary key column, no ORDER BY';
SELECT count(), countIf(l.c = r.c AND l.sb = r.sb)
FROM (SELECT a, count() AS c, sum(b) AS sb FROM t_gb_dyn GROUP BY a LIMIT 3) AS l
INNER JOIN ref_a AS r USING (a)
SETTINGS log_comment = '05233_a';

SELECT 'plan: the read is filtered by the boundary';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT a, count() FROM t_gb_dyn GROUP BY a LIMIT 3)
WHERE explain LIKE '%Prewhere filter column:%\_\_topKFilter(a)%';

SELECT 'second primary key column, no ORDER BY';
SELECT count(), countIf(l.c = r.c AND l.sa = r.sa)
FROM (SELECT b, count() AS c, sum(a) AS sa FROM t_gb_dyn GROUP BY b LIMIT 5) AS l
INNER JOIN ref_b AS r USING (b)
SETTINGS log_comment = '05233_b';

SELECT 'second primary key column, ORDER BY key DESC';
SELECT b, count() FROM t_gb_dyn GROUP BY b ORDER BY b DESC LIMIT 4
SETTINGS log_comment = '05233_b_desc';

SELECT 'WHERE moved to PREWHERE, conjoined with the boundary filter';
SELECT count(), countIf(l.c = r.c AND l.sb = r.sb)
FROM (SELECT a, count() AS c, sum(b) AS sb FROM t_gb_dyn WHERE s != '' GROUP BY a LIMIT 3) AS l
INNER JOIN ref_a AS r USING (a);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT a, count() FROM t_gb_dyn WHERE s != '' GROUP BY a LIMIT 3)
WHERE explain LIKE '%Prewhere filter column:%\_\_topKFilter(a)%' AND explain LIKE '% AND %';

SELECT 'explicit PREWHERE';
SELECT count(), countIf(c = 500)
FROM (SELECT a, count() AS c FROM t_gb_dyn PREWHERE b < 500 GROUP BY a LIMIT 3);

SELECT 'composite key: the boundary of the first key column';
SELECT count(), countIf(l.c = r.c)
FROM (SELECT a, intDiv(b, 100) AS bb, count() AS c FROM t_gb_dyn GROUP BY a, bb LIMIT 5) AS l
INNER JOIN ref_ab AS r USING (a, bb)
SETTINGS log_comment = '05233_ab';

SELECT 'Nullable key outside the primary key: filtered, not skipped';
SELECT count(), countIf(l.c = r.c)
FROM (SELECT n, count() AS c FROM t_gb_dyn GROUP BY n LIMIT 3) AS l
INNER JOIN ref_n AS r ON l.n IS NOT DISTINCT FROM r.n;

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT n, count() FROM t_gb_dyn GROUP BY n LIMIT 3)
WHERE explain LIKE '%Prewhere filter column:%\_\_topKFilter(n)%';

SELECT 'several aggregation streams publish into one tracker';
SELECT count(), countIf(l.c = r.c AND l.sa = r.sa)
FROM (SELECT b, count() AS c, sum(a) AS sa FROM t_gb_dyn GROUP BY b LIMIT 7) AS l
INNER JOIN ref_b AS r USING (b)
SETTINGS max_threads = 4;

SELECT 'ORDER BY column LIMIT n over the second primary key column skips granules too';
SELECT a, b FROM t_gb_dyn ORDER BY b, a LIMIT 3
SETTINGS log_comment = '05233_sort';

-- Not applied: the setting is off, the key is an expression over a column, the key is variable-length.
SELECT 'not applied';
SELECT count() FROM (EXPLAIN actions = 1 SELECT a, count() FROM t_gb_dyn GROUP BY a LIMIT 3 SETTINGS enable_group_by_top_k_dynamic_filtering = 0)
WHERE explain LIKE '%topKFilter%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT a + 1 AS k, count() FROM t_gb_dyn GROUP BY k LIMIT 3)
WHERE explain LIKE '%topKFilter%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT s, count() FROM t_gb_dyn GROUP BY s LIMIT 3)
WHERE explain LIKE '%topKFilter%';

SYSTEM FLUSH LOGS query_log;

-- The boundary is published after a few blocks of 1024 rows, and from then on the primary index
-- skips the granules beyond it: only a small part of the 100000 rows is read.
SELECT 'granules skipped by the primary key';
SELECT log_comment, read_rows < 50000, ProfileEvents['TopKGranulesSkippedByPrimaryKey'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05233_a', '05233_ab', '05233_b', '05233_b_desc', '05233_sort')
ORDER BY log_comment;

DROP TABLE t_gb_dyn;
DROP TABLE ref_a;
DROP TABLE ref_b;
DROP TABLE ref_ab;
DROP TABLE ref_n;
