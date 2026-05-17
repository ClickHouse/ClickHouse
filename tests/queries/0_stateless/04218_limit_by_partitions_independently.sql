-- Tags: long, no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: Explain output may differ


SET max_threads = 16;

-- { echo }

DROP TABLE IF EXISTS test_partition_eq_key;
CREATE TABLE test_partition_eq_key (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_partition_eq_key;
INSERT INTO test_partition_eq_key SELECT number, number FROM numbers_mt(1e5);
INSERT INTO test_partition_eq_key SELECT number, number FROM numbers_mt(1e5);
EXPLAIN PIPELINE SELECT a FROM test_partition_eq_key LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1;
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_partition_eq_key LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT a FROM test_partition_eq_key LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT a FROM test_partition_eq_key LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1));
DROP TABLE test_partition_eq_key;

DROP TABLE IF EXISTS test_partition_func_of_key;
CREATE TABLE test_partition_func_of_key (d Date, x UInt32) ENGINE = MergeTree ORDER BY d PARTITION BY toYYYYMM(d);
SYSTEM STOP MERGES test_partition_func_of_key;
INSERT INTO test_partition_func_of_key SELECT toDate('2024-01-01') + (number % 240), number FROM numbers_mt(5e4);
INSERT INTO test_partition_func_of_key SELECT toDate('2024-01-01') + (number % 240), number + 1 FROM numbers_mt(5e4);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT d FROM test_partition_func_of_key LIMIT 3 BY d SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT d FROM test_partition_func_of_key LIMIT 3 BY d SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT d FROM test_partition_func_of_key LIMIT 3 BY d SETTINGS allow_limit_by_partitions_independently = 1));
DROP TABLE test_partition_func_of_key;

DROP TABLE IF EXISTS test_complex_expr_both_sides;
CREATE TABLE test_complex_expr_both_sides (a UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY intDiv(a, 2) * 2 + 1;
INSERT INTO test_complex_expr_both_sides SELECT number FROM numbers_mt(100);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT intDiv(a, 2) + 1 AS a1 FROM test_complex_expr_both_sides LIMIT 1 BY a1 SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT intDiv(a, 2) + 1 AS a1 FROM test_complex_expr_both_sides LIMIT 1 BY a1 SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT intDiv(a, 2) + 1 AS a1 FROM test_complex_expr_both_sides LIMIT 1 BY a1 SETTINGS allow_limit_by_partitions_independently = 1));
DROP TABLE test_complex_expr_both_sides;

DROP TABLE IF EXISTS test_multi_col_partition_covered;
CREATE TABLE test_multi_col_partition_covered (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY (intDiv(a, 2), intDiv(b, 3));
INSERT INTO test_multi_col_partition_covered SELECT number, number FROM numbers_mt(100);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT intDiv(a, 2) + 1 AS a1, intDiv(b, 3) AS b1 FROM test_multi_col_partition_covered LIMIT 1 BY a1, b1 SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT intDiv(a, 2) + 1 AS a1, intDiv(b, 3) AS b1 FROM test_multi_col_partition_covered LIMIT 1 BY a1, b1 SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT intDiv(a, 2) + 1 AS a1, intDiv(b, 3) AS b1 FROM test_multi_col_partition_covered LIMIT 1 BY a1, b1 SETTINGS allow_limit_by_partitions_independently = 1));
DROP TABLE test_multi_col_partition_covered;

DROP TABLE IF EXISTS test_key_superset_partition;
CREATE TABLE test_key_superset_partition (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY a % 16;
INSERT INTO test_key_superset_partition SELECT number, number % 50 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, b FROM test_key_superset_partition LIMIT 2 BY a, b, pi() SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT a, b FROM test_key_superset_partition LIMIT 2 BY a, b, pi() SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT a, b FROM test_key_superset_partition LIMIT 2 BY a, b, pi() SETTINGS allow_limit_by_partitions_independently = 1));
DROP TABLE test_key_superset_partition;

DROP TABLE IF EXISTS test_partition_sum_of_components;
CREATE TABLE test_partition_sum_of_components (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY intDiv(a, 2) + intDiv(b, 3);
INSERT INTO test_partition_sum_of_components SELECT number, number FROM numbers_mt(100);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT intDiv(a, 2) AS a1, intDiv(b, 3) AS b1 FROM test_partition_sum_of_components LIMIT 1 BY a1, b1 SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT intDiv(a, 2) AS a1, intDiv(b, 3) AS b1 FROM test_partition_sum_of_components LIMIT 1 BY a1, b1 SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT intDiv(a, 2) AS a1, intDiv(b, 3) AS b1 FROM test_partition_sum_of_components LIMIT 1 BY a1, b1 SETTINGS allow_limit_by_partitions_independently = 1));
DROP TABLE test_partition_sum_of_components;

DROP TABLE IF EXISTS test_with_where_filter;
CREATE TABLE test_with_where_filter (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY b PARTITION BY a % 8;
SYSTEM STOP MERGES test_with_where_filter;
INSERT INTO test_with_where_filter SELECT number, number FROM numbers_mt(1e4);
INSERT INTO test_with_where_filter SELECT number, number + 1 FROM numbers_mt(1e4);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_with_where_filter WHERE b > 0 LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT a FROM test_with_where_filter WHERE b > 0 LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT a FROM test_with_where_filter WHERE b > 0 LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1));
DROP TABLE test_with_where_filter;

DROP TABLE IF EXISTS test_with_projection_alias;
CREATE TABLE test_with_projection_alias (a UInt32, payload String) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO test_with_projection_alias SELECT number, toString(number) FROM numbers_mt(1e4);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, length(payload) AS plen FROM test_with_projection_alias LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT a, length(payload) AS plen FROM test_with_projection_alias LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT a, length(payload) AS plen FROM test_with_projection_alias LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1));
DROP TABLE test_with_projection_alias;

DROP TABLE IF EXISTS test_injective_wrapper;
CREATE TABLE test_injective_wrapper (user_id UInt32, payload String) ENGINE = MergeTree ORDER BY tuple() PARTITION BY user_id % 8;
INSERT INTO test_injective_wrapper SELECT number, toString(number) FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT user_id FROM test_injective_wrapper LIMIT 10 BY toString(user_id) SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT user_id FROM test_injective_wrapper LIMIT 10 BY toString(user_id) SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT user_id FROM test_injective_wrapper LIMIT 10 BY toString(user_id) SETTINGS allow_limit_by_partitions_independently = 1));
DROP TABLE test_injective_wrapper;

DROP TABLE IF EXISTS test_summing_merge_tree;
CREATE TABLE test_summing_merge_tree (a UInt32, v UInt32) ENGINE = SummingMergeTree ORDER BY a PARTITION BY a % 8;
INSERT INTO test_summing_merge_tree SELECT number, 1 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_summing_merge_tree LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT a FROM test_summing_merge_tree LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT a FROM test_summing_merge_tree LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1));
DROP TABLE test_summing_merge_tree;

DROP TABLE IF EXISTS test_with_offset;
CREATE TABLE test_with_offset (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 4;
SYSTEM STOP MERGES test_with_offset;
INSERT INTO test_with_offset SELECT number % 20 FROM numbers_mt(1e3);
INSERT INTO test_with_offset SELECT number % 20 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_with_offset LIMIT 10 OFFSET 5 BY a SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT a FROM test_with_offset LIMIT 10 OFFSET 5 BY a SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT a FROM test_with_offset LIMIT 10 OFFSET 5 BY a SETTINGS allow_limit_by_partitions_independently = 1));
DROP TABLE test_with_offset;

DROP TABLE IF EXISTS test_few_partitions_many_threads;
CREATE TABLE test_few_partitions_many_threads (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 2;
SYSTEM STOP MERGES test_few_partitions_many_threads;
INSERT INTO test_few_partitions_many_threads SELECT number FROM numbers_mt(1e4);
INSERT INTO test_few_partitions_many_threads SELECT number FROM numbers_mt(1e4);
EXPLAIN PIPELINE SELECT a FROM test_few_partitions_many_threads LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1;
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_few_partitions_many_threads LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT a FROM test_few_partitions_many_threads LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT a FROM test_few_partitions_many_threads LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1));
DROP TABLE test_few_partitions_many_threads;

DROP TABLE IF EXISTS test_table_level_order_by;
CREATE TABLE test_table_level_order_by (a UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY a % 8;
SYSTEM STOP MERGES test_table_level_order_by;
INSERT INTO test_table_level_order_by SELECT number FROM numbers_mt(1e4);
INSERT INTO test_table_level_order_by SELECT number FROM numbers_mt(1e4);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_table_level_order_by LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT a FROM test_table_level_order_by LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT a FROM test_table_level_order_by LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1));
DROP TABLE test_table_level_order_by;

DROP TABLE IF EXISTS test_key_excludes_partition_col;
CREATE TABLE test_key_excludes_partition_col (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY (a % 4, b % 2);
INSERT INTO test_key_excludes_partition_col SELECT number, number FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_key_excludes_partition_col LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_key_excludes_partition_col;

DROP TABLE IF EXISTS test_key_noninjective_of_partition;
CREATE TABLE test_key_noninjective_of_partition (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a;
INSERT INTO test_key_noninjective_of_partition SELECT number % 8 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_key_noninjective_of_partition LIMIT 1 BY (a % 4) SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_key_noninjective_of_partition;

DROP TABLE IF EXISTS test_key_coarser_than_partition;
CREATE TABLE test_key_coarser_than_partition (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a;
INSERT INTO test_key_coarser_than_partition SELECT number % 16 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT intDiv(a, 2) AS a1 FROM test_key_coarser_than_partition LIMIT 1 BY a1 SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_key_coarser_than_partition;

DROP TABLE IF EXISTS test_mismatched_divisors;
CREATE TABLE test_mismatched_divisors (a UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY intDiv(a, 2);
INSERT INTO test_mismatched_divisors SELECT number FROM numbers_mt(100);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT intDiv(a, 3) AS a1 FROM test_mismatched_divisors LIMIT 1 BY a1 SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_mismatched_divisors;

DROP TABLE IF EXISTS test_noninjective_wrapper_in_key;
CREATE TABLE test_noninjective_wrapper_in_key (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY (intDiv(a, 2), intDiv(b, 3));
INSERT INTO test_noninjective_wrapper_in_key SELECT number, number FROM numbers_mt(100);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT intDiv(a, 2) + 1 AS a1, intDiv(b, 3) * 2 AS b1 FROM test_noninjective_wrapper_in_key LIMIT 1 BY a1, b1 SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_noninjective_wrapper_in_key;

DROP TABLE IF EXISTS test_nondeterministic_key;
CREATE TABLE test_nondeterministic_key (a UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY a % 8;
INSERT INTO test_nondeterministic_key SELECT number FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_nondeterministic_key LIMIT 1 BY rand(a) SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_nondeterministic_key;

DROP TABLE IF EXISTS test_stateful_key;
CREATE TABLE test_stateful_key (a UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY a % 8;
INSERT INTO test_stateful_key SELECT number FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_stateful_key LIMIT 1 BY blockNumber() SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_stateful_key;

DROP TABLE IF EXISTS test_no_partition_by;
CREATE TABLE test_no_partition_by (a UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO test_no_partition_by SELECT number FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_no_partition_by LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_no_partition_by;

DROP TABLE IF EXISTS test_with_order_by_in_query;
CREATE TABLE test_with_order_by_in_query (a UInt32, score UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO test_with_order_by_in_query SELECT number, number FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_with_order_by_in_query ORDER BY score DESC LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_with_order_by_in_query;

DROP TABLE IF EXISTS test_with_final;
CREATE TABLE test_with_final (a UInt32, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY a PARTITION BY a % 8;
INSERT INTO test_with_final SELECT number, 1 FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_with_final FINAL LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_with_final;

DROP TABLE IF EXISTS test_projection_partitioned_reading;
CREATE TABLE test_projection_partitioned_reading (a UInt32, b UInt32, PROJECTION p (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY b PARTITION BY a % 8;
SYSTEM STOP MERGES test_projection_partitioned_reading;
INSERT INTO test_projection_partitioned_reading SELECT number % 100, number FROM numbers_mt(10000);
INSERT INTO test_projection_partitioned_reading SELECT number % 100, number + 10000 FROM numbers_mt(10000);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM test_projection_partitioned_reading LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1, optimize_use_projections = 1, force_optimize_projection = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT a FROM test_projection_partitioned_reading LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 0, optimize_use_projections = 1, force_optimize_projection = 1)) = (SELECT count() FROM (SELECT a FROM test_projection_partitioned_reading LIMIT 2 BY a SETTINGS allow_limit_by_partitions_independently = 1, optimize_use_projections = 1, force_optimize_projection = 1));
DROP TABLE test_projection_partitioned_reading;
