-- A distributed aggregation with many groups aggregates partially where the data is read, shuffles
-- the partial states by the group keys and merges them per bucket, instead of shuffling every input row.

DROP TABLE IF EXISTS t_shuffle_states;
CREATE TABLE t_shuffle_states (k UInt32, s String, n Nullable(UInt8), v Int64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_shuffle_states;
INSERT INTO t_shuffle_states SELECT number % 5000, toString(number % 7), if(number % 11 = 0, NULL, number % 13), number FROM numbers(100000);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_default_reader_bucket_count = 4;
SET distributed_plan_default_shuffle_join_bucket_count = 4;
SET distributed_plan_force_shuffle_aggregation = 0;

SELECT '-- rule-based: partial aggregation, shuffle of the states, merge per bucket';
EXPLAIN SELECT k, sum(v) FROM t_shuffle_states GROUP BY k SETTINGS enable_cascades_optimizer = 0;

SELECT '-- rule-based, setting off: shuffle of the input rows';
EXPLAIN SELECT k, sum(v) FROM t_shuffle_states GROUP BY k
SETTINGS enable_cascades_optimizer = 0, distributed_plan_partial_aggregation_before_shuffle = 0;

SELECT '-- rule-based, forced shuffle: shuffle of the input rows, as in the Cascades optimizer';
EXPLAIN SELECT k, sum(v) FROM t_shuffle_states GROUP BY k
SETTINGS enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1;

SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{"t_shuffle_states": {"cardinality": 100000000, "avg_row_bytes": 16, "distinct_keys": {"k": 10000000}}}';

SELECT '-- Cascades, many groups: the merge runs per bucket over shuffled states';
EXPLAIN SELECT k, sum(v) FROM t_shuffle_states GROUP BY k SETTINGS enable_cascades_optimizer = 1;

SELECT '-- Cascades, many groups, setting off: shuffle of the input rows';
EXPLAIN SELECT k, sum(v) FROM t_shuffle_states GROUP BY k
SETTINGS enable_cascades_optimizer = 1, distributed_plan_partial_aggregation_before_shuffle = 0;

SET param__internal_join_table_stat_hints = '{"t_shuffle_states": {"cardinality": 100000000, "avg_row_bytes": 16, "distinct_keys": {"k": 100}}}';

SELECT '-- Cascades, few groups: the merge stays on one node';
EXPLAIN SELECT k, sum(v) FROM t_shuffle_states GROUP BY k SETTINGS enable_cascades_optimizer = 1;

SET param__internal_join_table_stat_hints = '{"t_shuffle_states": {"cardinality": 100000000, "avg_row_bytes": 16, "distinct_keys": {"k": 10000000, "n": 14}}}';

SELECT '-- results match the local execution';
-- Each query prints one fingerprint of its whole result; the lines of one query must be equal.
-- The hints above make Cascades merge per bucket here too.
SELECT count(), groupBitXor(cityHash64(k, sum_v, cnt, uniq_s, avg_v, max_s, top_v)) FROM (SELECT k, sum(v) AS sum_v, count() AS cnt, uniqExact(s) AS uniq_s, avg(v) AS avg_v, max(s) AS max_s, groupArraySorted(3)(v) AS top_v FROM t_shuffle_states GROUP BY k) SETTINGS make_distributed_plan = 0;
SELECT count(), groupBitXor(cityHash64(k, sum_v, cnt, uniq_s, avg_v, max_s, top_v)) FROM (SELECT k, sum(v) AS sum_v, count() AS cnt, uniqExact(s) AS uniq_s, avg(v) AS avg_v, max(s) AS max_s, groupArraySorted(3)(v) AS top_v FROM t_shuffle_states GROUP BY k) SETTINGS enable_cascades_optimizer = 0;
SELECT count(), groupBitXor(cityHash64(k, sum_v, cnt, uniq_s, avg_v, max_s, top_v)) FROM (SELECT k, sum(v) AS sum_v, count() AS cnt, uniqExact(s) AS uniq_s, avg(v) AS avg_v, max(s) AS max_s, groupArraySorted(3)(v) AS top_v FROM t_shuffle_states GROUP BY k) SETTINGS enable_cascades_optimizer = 1;

SELECT '-- two keys, a nullable key and HAVING';
SELECT count(), groupBitXor(cityHash64(kk, n, sum_v, uniq_s)) FROM (SELECT k % 300 AS kk, n, sum(v) AS sum_v, uniqExact(s) AS uniq_s FROM t_shuffle_states GROUP BY kk, n HAVING sum_v > 10000) SETTINGS make_distributed_plan = 0;
SELECT count(), groupBitXor(cityHash64(kk, n, sum_v, uniq_s)) FROM (SELECT k % 300 AS kk, n, sum(v) AS sum_v, uniqExact(s) AS uniq_s FROM t_shuffle_states GROUP BY kk, n HAVING sum_v > 10000) SETTINGS enable_cascades_optimizer = 0;
SELECT count(), groupBitXor(cityHash64(kk, n, sum_v, uniq_s)) FROM (SELECT k % 300 AS kk, n, sum(v) AS sum_v, uniqExact(s) AS uniq_s FROM t_shuffle_states GROUP BY kk, n HAVING sum_v > 10000) SETTINGS enable_cascades_optimizer = 1;

DROP TABLE t_shuffle_states;
