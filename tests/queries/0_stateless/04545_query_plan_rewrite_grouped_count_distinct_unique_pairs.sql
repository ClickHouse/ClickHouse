-- Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- The test asserts the local query-plan shape, which parallel replicas would change. Random
-- settings are excluded because the statistics gate decides based on the execution topology,
-- which randomized read/aggregation settings legitimately change.

-- Both sides of the gate's pair-duplication condition. When every (group key, argument value)
-- pair is unique, the deduplicating aggregation removes nothing and the rewrite only adds work —
-- the gate must keep it off. When pairs are heavily duplicated, the rewrite pays off and the
-- decision must survive rewritten executions: the created aggregations record the group-key,
-- pair, and source-row counts back onto the original aggregation's entry, and those must not be
-- mistaken for an unfavorable data shape.

SET query_plan_rewrite_grouped_count_distinct = 1;
SET max_rows_to_group_by = 0;
-- The gate requires group keys shared across several reading streams; the thread count is pinned
-- so the table is read in the same number of streams on any machine.
SET max_threads = 4;

DROP TABLE IF EXISTS t_cd_unique_pairs;
DROP TABLE IF EXISTS t_cd_dup_pairs;

SELECT 'unique pairs never fire';
CREATE TABLE t_cd_unique_pairs (k UInt32, x UInt64) ENGINE = MergeTree ORDER BY tuple()
AS SELECT number % 10, number FROM numbers(1000000);
SELECT k, uniqExact(x) FROM t_cd_unique_pairs GROUP BY k ORDER BY k LIMIT 1;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(x) FROM t_cd_unique_pairs GROUP BY k) WHERE explain LIKE '%Aggregating%';

SELECT 'duplicated pairs stay on across rewritten executions';
-- Every (k, x) pair occurs 20 times, spread 50000 rows apart so all reading streams see it.
CREATE TABLE t_cd_dup_pairs (k UInt32, x UInt64) ENGINE = MergeTree ORDER BY tuple()
AS SELECT number % 10, intHash64(number % 50000) FROM numbers(1000000);
SELECT k, uniqExact(x) FROM t_cd_dup_pairs GROUP BY k ORDER BY k LIMIT 1;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(x) FROM t_cd_dup_pairs GROUP BY k) WHERE explain LIKE '%Aggregating%';
SELECT k, uniqExact(x) FROM t_cd_dup_pairs GROUP BY k ORDER BY k LIMIT 1;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(x) FROM t_cd_dup_pairs GROUP BY k) WHERE explain LIKE '%Aggregating%';
SELECT k, uniqExact(x) FROM t_cd_dup_pairs GROUP BY k ORDER BY k LIMIT 1;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(x) FROM t_cd_dup_pairs GROUP BY k) WHERE explain LIKE '%Aggregating%';

DROP TABLE t_cd_unique_pairs;
DROP TABLE t_cd_dup_pairs;
