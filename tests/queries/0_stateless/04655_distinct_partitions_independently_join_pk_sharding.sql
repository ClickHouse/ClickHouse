-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: Explain output may differ

-- The independent-partitions optimization is disabled under parallel replicas.
SET enable_parallel_replicas = 0;

-- Some CI configurations set DISTINCT size limits at the server level; pin them to unlimited so that
-- independent per-partition DISTINCT is applied.
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;

-- The pretty EXPLAIN output decorates plan lines with tree-drawing characters; use the legacy format
-- so the assertions below match plain marker lines.
SET explain_query_plan_default = 'legacy';

SET max_threads = 8;
SET allow_distinct_partitions_independently = 1;
SET force_distinct_partitions_independently = 1;
SET query_plan_join_shard_by_pk_ranges = 1;

-- JOIN sharding by primary-key ranges applies only to plain hash / concurrent hash / full sorting merge
-- joins over unmodified MergeTree reads, so disable the features that would wrap or reshape them.
SET enable_join_runtime_filters = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;

DROP TABLE IF EXISTS t_distinct_join_l;
DROP TABLE IF EXISTS t_distinct_join_r;
CREATE TABLE t_distinct_join_l (a UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY a % 8;
CREATE TABLE t_distinct_join_r (a UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_distinct_join_l SELECT number FROM numbers(800);
INSERT INTO t_distinct_join_r SELECT number FROM numbers(800);

-- Reading each partition through a separate port (independent DISTINCT) and sharding the JOIN by
-- primary-key-range layers reshape the same read in incompatible ways, so they must be mutually
-- exclusive. The read is claimed by the independent DISTINCT first, and the JOIN sharding backs off:
-- the plan contains the partition port marker and no `Sharding` marker.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN actions = 1 SELECT count() FROM (SELECT DISTINCT a FROM t_distinct_join_l) AS l INNER JOIN t_distinct_join_r AS r ON l.a = r.a) WHERE explain LIKE '%Sharding%' OR explain LIKE '%Read each partition through separate port%';
SELECT count() FROM (SELECT DISTINCT a FROM t_distinct_join_l) AS l INNER JOIN t_distinct_join_r AS r ON l.a = r.a;

-- With independent DISTINCT disabled the same query is sharded by primary-key ranges.
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN actions = 1 SELECT count() FROM (SELECT DISTINCT a FROM t_distinct_join_l) AS l INNER JOIN t_distinct_join_r AS r ON l.a = r.a SETTINGS allow_distinct_partitions_independently = 0, force_distinct_partitions_independently = 0) WHERE explain LIKE '%Sharding%' OR explain LIKE '%Read each partition through separate port%';
SELECT count() FROM (SELECT DISTINCT a FROM t_distinct_join_l) AS l INNER JOIN t_distinct_join_r AS r ON l.a = r.a SETTINGS allow_distinct_partitions_independently = 0, force_distinct_partitions_independently = 0;

DROP TABLE t_distinct_join_l;
DROP TABLE t_distinct_join_r;
