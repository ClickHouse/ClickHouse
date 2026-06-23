-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: Explain output may differ

-- max_threads = 8: the cost heuristic requires partitions >= max_threads/2, and every positive case
-- below has >= 8 balanced partitions. The heuristic itself (too few / too many / skewed partitions)
-- is covered separately in 04414.
SET max_threads = 8;
-- The optimization is disabled under parallel replicas.
SET enable_parallel_replicas = 0;

-- { echo }

-- partition key equals the DISTINCT key
DROP TABLE IF EXISTS test_partition_eq_key;
CREATE TABLE test_partition_eq_key (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_partition_eq_key;
INSERT INTO test_partition_eq_key SELECT number, number FROM numbers_mt(1e3);
INSERT INTO test_partition_eq_key SELECT number, number FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_partition_eq_key SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM test_partition_eq_key SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM test_partition_eq_key SETTINGS allow_distinct_partitions_independently = 1));
DROP TABLE test_partition_eq_key;

-- partition key is a function of the DISTINCT key
DROP TABLE IF EXISTS test_partition_func_of_key;
CREATE TABLE test_partition_func_of_key (d Date, x UInt32) ENGINE = MergeTree ORDER BY d PARTITION BY toYYYYMM(d);
SYSTEM STOP MERGES test_partition_func_of_key;
INSERT INTO test_partition_func_of_key SELECT toDate('2024-01-01') + (number % 240), number FROM numbers_mt(1e3);
INSERT INTO test_partition_func_of_key SELECT toDate('2024-01-01') + (number % 240), number + 1 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT d FROM test_partition_func_of_key SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT d FROM test_partition_func_of_key SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT d FROM test_partition_func_of_key SETTINGS allow_distinct_partitions_independently = 1));
DROP TABLE test_partition_func_of_key;

-- the DISTINCT key is computed by a projection (determines the partition)
DROP TABLE IF EXISTS test_complex_expr_both_sides;
CREATE TABLE test_complex_expr_both_sides (a UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY intDiv(a, 2) * 2 + 1;
INSERT INTO test_complex_expr_both_sides SELECT number FROM numbers_mt(100);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT intDiv(a, 2) + 1 AS a1 FROM test_complex_expr_both_sides SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT intDiv(a, 2) + 1 AS a1 FROM test_complex_expr_both_sides SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT intDiv(a, 2) + 1 AS a1 FROM test_complex_expr_both_sides SETTINGS allow_distinct_partitions_independently = 1));
DROP TABLE test_complex_expr_both_sides;

-- multi-column partition fully covered by the DISTINCT key
DROP TABLE IF EXISTS test_multi_col_partition_covered;
CREATE TABLE test_multi_col_partition_covered (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY (intDiv(a, 2), intDiv(b, 3));
INSERT INTO test_multi_col_partition_covered SELECT number, number FROM numbers_mt(100);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT intDiv(a, 2) + 1 AS a1, intDiv(b, 3) AS b1 FROM test_multi_col_partition_covered SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT intDiv(a, 2) + 1 AS a1, intDiv(b, 3) AS b1 FROM test_multi_col_partition_covered SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT intDiv(a, 2) + 1 AS a1, intDiv(b, 3) AS b1 FROM test_multi_col_partition_covered SETTINGS allow_distinct_partitions_independently = 1));
DROP TABLE test_multi_col_partition_covered;

-- DISTINCT key is a superset of the partition columns
DROP TABLE IF EXISTS test_key_superset_partition;
CREATE TABLE test_key_superset_partition (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY a % 16;
INSERT INTO test_key_superset_partition SELECT number, number % 50 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a, b FROM test_key_superset_partition SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a, b FROM test_key_superset_partition SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a, b FROM test_key_superset_partition SETTINGS allow_distinct_partitions_independently = 1));
DROP TABLE test_key_superset_partition;

-- partition key is the sum of the two DISTINCT key components
DROP TABLE IF EXISTS test_partition_sum_of_components;
CREATE TABLE test_partition_sum_of_components (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY intDiv(a, 2) + intDiv(b, 3);
INSERT INTO test_partition_sum_of_components SELECT number, number FROM numbers_mt(100);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT intDiv(a, 2) AS a1, intDiv(b, 3) AS b1 FROM test_partition_sum_of_components SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT intDiv(a, 2) AS a1, intDiv(b, 3) AS b1 FROM test_partition_sum_of_components SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT intDiv(a, 2) AS a1, intDiv(b, 3) AS b1 FROM test_partition_sum_of_components SETTINGS allow_distinct_partitions_independently = 1));
DROP TABLE test_partition_sum_of_components;

-- WHERE clause (filter folded into the read)
DROP TABLE IF EXISTS test_with_where_filter;
CREATE TABLE test_with_where_filter (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY b PARTITION BY a % 8;
SYSTEM STOP MERGES test_with_where_filter;
INSERT INTO test_with_where_filter SELECT number, number FROM numbers_mt(1e3);
INSERT INTO test_with_where_filter SELECT number, number + 1 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_with_where_filter WHERE b > 0 SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM test_with_where_filter WHERE b > 0 SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM test_with_where_filter WHERE b > 0 SETTINGS allow_distinct_partitions_independently = 1));
DROP TABLE test_with_where_filter;

-- explicit Filter step (PREWHERE disabled)
DROP TABLE IF EXISTS test_with_filter_step;
CREATE TABLE test_with_filter_step (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY b PARTITION BY a % 8;
SYSTEM STOP MERGES test_with_filter_step;
INSERT INTO test_with_filter_step SELECT number, number FROM numbers_mt(1e3);
INSERT INTO test_with_filter_step SELECT number, number + 1 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_with_filter_step WHERE b > 0 SETTINGS allow_distinct_partitions_independently = 1, optimize_move_to_prewhere = 0) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM test_with_filter_step WHERE b > 0 SETTINGS allow_distinct_partitions_independently = 0, optimize_move_to_prewhere = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM test_with_filter_step WHERE b > 0 SETTINGS allow_distinct_partitions_independently = 1, optimize_move_to_prewhere = 0));
DROP TABLE test_with_filter_step;

-- extra non-partition column in the DISTINCT key plus a projection alias
DROP TABLE IF EXISTS test_with_projection_alias;
CREATE TABLE test_with_projection_alias (a UInt32, payload String) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO test_with_projection_alias SELECT number, toString(number) FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a, length(payload) AS plen FROM test_with_projection_alias SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a, length(payload) AS plen FROM test_with_projection_alias SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a, length(payload) AS plen FROM test_with_projection_alias SETTINGS allow_distinct_partitions_independently = 1));
DROP TABLE test_with_projection_alias;

-- injective function of the partition column in the DISTINCT key
DROP TABLE IF EXISTS test_injective_wrapper;
CREATE TABLE test_injective_wrapper (user_id UInt32, payload String) ENGINE = MergeTree ORDER BY tuple() PARTITION BY user_id % 8;
INSERT INTO test_injective_wrapper SELECT number, toString(number) FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT toString(user_id) FROM test_injective_wrapper SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT toString(user_id) FROM test_injective_wrapper SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT toString(user_id) FROM test_injective_wrapper SETTINGS allow_distinct_partitions_independently = 1));
DROP TABLE test_injective_wrapper;

-- SummingMergeTree
DROP TABLE IF EXISTS test_summing_merge_tree;
CREATE TABLE test_summing_merge_tree (a UInt32, v UInt32) ENGINE = SummingMergeTree ORDER BY a PARTITION BY a % 8;
INSERT INTO test_summing_merge_tree SELECT number, 1 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_summing_merge_tree SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM test_summing_merge_tree SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM test_summing_merge_tree SETTINGS allow_distinct_partitions_independently = 1));
DROP TABLE test_summing_merge_tree;

-- table-level ORDER BY (no query-level ORDER BY, so no Sorting before the final DISTINCT)
DROP TABLE IF EXISTS test_table_level_order_by;
CREATE TABLE test_table_level_order_by (a UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY a % 8;
SYSTEM STOP MERGES test_table_level_order_by;
INSERT INTO test_table_level_order_by SELECT number FROM numbers_mt(1e3);
INSERT INTO test_table_level_order_by SELECT number FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_table_level_order_by SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM test_table_level_order_by SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM test_table_level_order_by SETTINGS allow_distinct_partitions_independently = 1));
DROP TABLE test_table_level_order_by;

-- ARRAY JOIN is a transparent (per-stream) step: DISTINCT on a partition-determined column skips merging
DROP TABLE IF EXISTS test_array_join;
CREATE TABLE test_array_join (a UInt32, arr Array(UInt32)) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_array_join;
INSERT INTO test_array_join SELECT number, range(number % 5) FROM numbers_mt(1e3);
INSERT INTO test_array_join SELECT number, range(number % 5) FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_array_join ARRAY JOIN arr SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM test_array_join ARRAY JOIN arr SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM test_array_join ARRAY JOIN arr SETTINGS allow_distinct_partitions_independently = 1));
-- same with LEFT ARRAY JOIN
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_array_join LEFT ARRAY JOIN arr SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM test_array_join LEFT ARRAY JOIN arr SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM test_array_join LEFT ARRAY JOIN arr SETTINGS allow_distinct_partitions_independently = 1));
-- NEGATIVE: DISTINCT on the array-joined column only (not partition-determined)
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT arr FROM test_array_join ARRAY JOIN arr SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_array_join;

-- NEGATIVE: DISTINCT key excludes one partition column
DROP TABLE IF EXISTS test_key_excludes_partition_col;
CREATE TABLE test_key_excludes_partition_col (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY (a % 4, b % 2);
INSERT INTO test_key_excludes_partition_col SELECT number, number FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_key_excludes_partition_col SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_key_excludes_partition_col;

-- NEGATIVE: partition is finer than the DISTINCT key (key is non-injective function of the partition)
DROP TABLE IF EXISTS test_key_noninjective_of_partition;
CREATE TABLE test_key_noninjective_of_partition (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a;
INSERT INTO test_key_noninjective_of_partition SELECT number % 8 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a % 4 AS a1 FROM test_key_noninjective_of_partition SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_key_noninjective_of_partition;

-- NEGATIVE: DISTINCT key is coarser than the partition
DROP TABLE IF EXISTS test_key_coarser_than_partition;
CREATE TABLE test_key_coarser_than_partition (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a;
INSERT INTO test_key_coarser_than_partition SELECT number % 16 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT intDiv(a, 2) AS a1 FROM test_key_coarser_than_partition SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_key_coarser_than_partition;

-- NEGATIVE: mismatched divisors
DROP TABLE IF EXISTS test_mismatched_divisors;
CREATE TABLE test_mismatched_divisors (a UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY intDiv(a, 2);
INSERT INTO test_mismatched_divisors SELECT number FROM numbers_mt(100);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT intDiv(a, 3) AS a1 FROM test_mismatched_divisors SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_mismatched_divisors;

-- NEGATIVE: non-injective wrappers around the partition components in the DISTINCT key
DROP TABLE IF EXISTS test_noninjective_wrapper_in_key;
CREATE TABLE test_noninjective_wrapper_in_key (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY (intDiv(a, 2), intDiv(b, 3));
INSERT INTO test_noninjective_wrapper_in_key SELECT number, number FROM numbers_mt(100);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT intDiv(a, 4) AS a1, intDiv(b, 9) AS b1 FROM test_noninjective_wrapper_in_key SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_noninjective_wrapper_in_key;

-- NEGATIVE: non-deterministic key
DROP TABLE IF EXISTS test_nondeterministic_key;
CREATE TABLE test_nondeterministic_key (a UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY a % 8;
INSERT INTO test_nondeterministic_key SELECT number FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT rand(a) AS r FROM test_nondeterministic_key SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_nondeterministic_key;

-- NEGATIVE: stateful key
DROP TABLE IF EXISTS test_stateful_key;
CREATE TABLE test_stateful_key (a UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY a % 8;
INSERT INTO test_stateful_key SELECT number FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT blockNumber() AS bn FROM test_stateful_key SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_stateful_key;

-- NEGATIVE: only one partition (per-partition reading is pointless, so it is not requested)
DROP TABLE IF EXISTS test_single_partition;
CREATE TABLE test_single_partition (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_single_partition;
INSERT INTO test_single_partition SELECT 8 * number FROM numbers_mt(1e3);
INSERT INTO test_single_partition SELECT 8 * number FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_single_partition SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM test_single_partition SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM test_single_partition SETTINGS allow_distinct_partitions_independently = 1));
DROP TABLE test_single_partition;

-- NEGATIVE: no PARTITION BY
DROP TABLE IF EXISTS test_no_partition_by;
CREATE TABLE test_no_partition_by (a UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO test_no_partition_by SELECT number FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_no_partition_by SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_no_partition_by;

-- NEGATIVE: FINAL
DROP TABLE IF EXISTS test_with_final;
CREATE TABLE test_with_final (a UInt32, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY a PARTITION BY a % 8;
INSERT INTO test_with_final SELECT number, 1 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_with_final FINAL SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_with_final;

-- NEGATIVE: reading through a projection
DROP TABLE IF EXISTS test_projection_partitioned_reading;
CREATE TABLE test_projection_partitioned_reading (a UInt32, b UInt32, PROJECTION p (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY b PARTITION BY a % 8;
SYSTEM STOP MERGES test_projection_partitioned_reading;
INSERT INTO test_projection_partitioned_reading SELECT number % 100, number FROM numbers_mt(1e3);
INSERT INTO test_projection_partitioned_reading SELECT number % 100, number + 10000 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_projection_partitioned_reading SETTINGS allow_distinct_partitions_independently = 1, optimize_use_projections = 1, force_optimize_projection = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM test_projection_partitioned_reading SETTINGS allow_distinct_partitions_independently = 0, optimize_use_projections = 1, force_optimize_projection = 1)) = (SELECT count() FROM (SELECT DISTINCT a FROM test_projection_partitioned_reading SETTINGS allow_distinct_partitions_independently = 1, optimize_use_projections = 1, force_optimize_projection = 1));
DROP TABLE test_projection_partitioned_reading;

-- ORDER BY: a Sorting re-mixes streams before the final DISTINCT, so the final merge is not skipped
-- (the preliminary DISTINCT still reads partitions through separate ports). Results must be identical.
DROP TABLE IF EXISTS test_with_order_by_in_query;
CREATE TABLE test_with_order_by_in_query (a UInt32, score UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO test_with_order_by_in_query SELECT number, number FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM test_with_order_by_in_query ORDER BY a SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT groupArray(a) FROM (SELECT DISTINCT a FROM test_with_order_by_in_query ORDER BY a SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT groupArray(a) FROM (SELECT DISTINCT a FROM test_with_order_by_in_query ORDER BY a SETTINGS allow_distinct_partitions_independently = 1));
DROP TABLE test_with_order_by_in_query;
