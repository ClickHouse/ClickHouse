-- Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- no-parallel-replicas, no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ


SET query_plan_rewrite_grouped_count_distinct = 1;

SET max_threads = 4;

DROP TABLE IF EXISTS t_grouped_uniq_exact;
CREATE TABLE t_grouped_uniq_exact (k UInt32, v UInt64, n Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple()
AS SELECT number % 10, intHash64(number) % 5000, if(number % 7 = 0, NULL, toUInt32(number % 3333)) FROM numbers(1000000);

SELECT 'cold run: no statistics yet, a single aggregation';
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k) WHERE explain LIKE '%Aggregating%';
SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k ORDER BY k;

SELECT 'warm run: rewritten into a count over a deduplicating aggregation, same result';
SELECT replaceRegexpAll(explain, '^[^A-Za-z]+', '') FROM (EXPLAIN SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k) WHERE explain LIKE '%Keys:%' OR explain LIKE '%Aggregates:%';
SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k ORDER BY k;

SELECT 'a NULL argument value does not count as a distinct value, exactly as in uniqExact';
SELECT k, uniqExact(n) FROM t_grouped_uniq_exact GROUP BY k ORDER BY k;
SELECT k, uniqExact(n) FROM t_grouped_uniq_exact GROUP BY k ORDER BY k;

SELECT 'the disabled setting suppresses the rewrite';
SET query_plan_rewrite_grouped_count_distinct = 0;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k) WHERE explain LIKE '%Aggregating%';
SET query_plan_rewrite_grouped_count_distinct = 1;

SELECT 'the analyzer setting alone does not rewrite grouped queries';
SET count_distinct_optimization = 1;
SET query_plan_rewrite_grouped_count_distinct = 0;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k) WHERE explain LIKE '%Aggregating%';
SET count_distinct_optimization = 0;
SET query_plan_rewrite_grouped_count_distinct = 1;

SELECT 'an argument that is itself a group key is not rewritten';
SELECT count() FROM (EXPLAIN SELECT v, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY v) WHERE explain LIKE '%Aggregating%';

SELECT 'WITH TOTALS is not rewritten';
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(v) FROM t_grouped_uniq_exact GROUP BY k WITH TOTALS) WHERE explain LIKE '%Aggregating%';

SELECT 'an observed group-key cardinality in the millions suppresses the rewrite';
DROP TABLE IF EXISTS t_many_group_keys;
CREATE TABLE t_many_group_keys (k UInt32, v UInt8) ENGINE = MergeTree ORDER BY tuple()
AS SELECT number, number % 3 FROM numbers(2000000);
SELECT k, uniqExact(v) FROM t_many_group_keys GROUP BY k FORMAT Null;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(v) FROM t_many_group_keys GROUP BY k) WHERE explain LIKE '%Aggregating%';
DROP TABLE t_many_group_keys;

DROP TABLE t_grouped_uniq_exact;
