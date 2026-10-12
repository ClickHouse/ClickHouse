-- Tags: long, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-random-settings, no-random-merge-tree-settings, no-parallel-replicas: Explain output may differ

SET max_threads = 16;

-- { echo }

-- Optimized: LIMIT BY length-1 PK prefix
DROP TABLE IF EXISTS test_pk_prefix_1col;
CREATE TABLE test_pk_prefix_1col (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_pk_prefix_1col SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_pk_prefix_1col LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_pk_prefix_1col LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_pk_prefix_1col LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_pk_prefix_1col;

-- Optimized: LIMIT BY length-2 PK prefix
DROP TABLE IF EXISTS test_pk_prefix_2col;
CREATE TABLE test_pk_prefix_2col (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_pk_prefix_2col SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_pk_prefix_2col LIMIT 5 BY a, b SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_pk_prefix_2col LIMIT 5 BY a, b SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_pk_prefix_2col LIMIT 5 BY a, b SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_pk_prefix_2col;

-- Optimized: LIMIT BY full PK
DROP TABLE IF EXISTS test_pk_prefix_full;
CREATE TABLE test_pk_prefix_full (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_pk_prefix_full SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_pk_prefix_full LIMIT 5 BY a, b, c SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_pk_prefix_full LIMIT 5 BY a, b, c SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_pk_prefix_full LIMIT 5 BY a, b, c SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_pk_prefix_full;

-- Optimized: WHERE fixes leading PK column
DROP TABLE IF EXISTS test_where_fixed;
CREATE TABLE test_where_fixed (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_where_fixed SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_where_fixed WHERE a = 1 LIMIT 5 BY b SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_where_fixed WHERE a = 1 LIMIT 5 BY b SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_where_fixed WHERE a = 1 LIMIT 5 BY b SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_where_fixed;

-- Optimized: WHERE on non-PK column does not block detection
DROP TABLE IF EXISTS test_where_irrelevant;
CREATE TABLE test_where_irrelevant (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_where_irrelevant SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_where_irrelevant WHERE z > 5 LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_where_irrelevant WHERE z > 5 LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_where_irrelevant WHERE z > 5 LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_where_irrelevant;

-- Optimized: monotonic function on leading PK column
DROP TABLE IF EXISTS test_monotonic_function;
CREATE TABLE test_monotonic_function (a DateTime, b UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO test_monotonic_function SELECT toDateTime(number % 100000), number % 97, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, z FROM test_monotonic_function LIMIT 5 BY toStartOfHour(a) SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, z FROM test_monotonic_function LIMIT 5 BY toStartOfHour(a) SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, z FROM test_monotonic_function LIMIT 5 BY toStartOfHour(a) SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_monotonic_function;

-- Optimized: strictly-monotonic negative function (reverse direction)
DROP TABLE IF EXISTS test_negative_monotonic;
CREATE TABLE test_negative_monotonic (a Int32, b UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO test_negative_monotonic SELECT number % 1000, number % 97, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, z FROM test_negative_monotonic LIMIT 5 BY negate(a) SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, z FROM test_negative_monotonic LIMIT 5 BY negate(a) SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, z FROM test_negative_monotonic LIMIT 5 BY negate(a) SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_negative_monotonic;

-- Optimized: LIMIT BY with OFFSET
DROP TABLE IF EXISTS test_with_offset;
CREATE TABLE test_with_offset (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_with_offset SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_with_offset LIMIT 5 OFFSET 2 BY a SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_with_offset LIMIT 5 OFFSET 2 BY a SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_with_offset LIMIT 5 OFFSET 2 BY a SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_with_offset;

-- Not optimized: BY is a middle PK column (no WHERE fixing `a`)
DROP TABLE IF EXISTS test_middle_column;
CREATE TABLE test_middle_column (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_middle_column SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_middle_column LIMIT 5 BY b SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_middle_column LIMIT 5 BY b SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_middle_column LIMIT 5 BY b SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_middle_column;

-- Not optimized: BY is the last PK column
DROP TABLE IF EXISTS test_last_column;
CREATE TABLE test_last_column (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_last_column SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_last_column LIMIT 5 BY c SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_last_column LIMIT 5 BY c SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_last_column LIMIT 5 BY c SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_last_column;

-- Not optimized: BY skips a PK column (a, c) instead of (a, b)
DROP TABLE IF EXISTS test_skip_prefix;
CREATE TABLE test_skip_prefix (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_skip_prefix SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_skip_prefix LIMIT 5 BY a, c SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_skip_prefix LIMIT 5 BY a, c SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_skip_prefix LIMIT 5 BY a, c SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_skip_prefix;

-- Not optimized: BY is a non-PK column
DROP TABLE IF EXISTS test_non_pk_column;
CREATE TABLE test_non_pk_column (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_non_pk_column SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_non_pk_column LIMIT 5 BY z SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_non_pk_column LIMIT 5 BY z SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_non_pk_column LIMIT 5 BY z SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_non_pk_column;

-- Not optimized: ORDER BY non-PK column blocks the pass (SortingStep is not crossed by findReadingStep)
DROP TABLE IF EXISTS test_order_by_non_pk;
CREATE TABLE test_order_by_non_pk (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_order_by_non_pk SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_order_by_non_pk ORDER BY z LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_order_by_non_pk ORDER BY z LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_order_by_non_pk ORDER BY z LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_order_by_non_pk;

-- Optimized: multi-column WHERE-fixed
DROP TABLE IF EXISTS test_multi_where_fixed;
CREATE TABLE test_multi_where_fixed (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_multi_where_fixed SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_multi_where_fixed WHERE a = 1 AND b = 2 LIMIT 5 BY c SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_multi_where_fixed WHERE a = 1 AND b = 2 LIMIT 5 BY c SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_multi_where_fixed WHERE a = 1 AND b = 2 LIMIT 5 BY c SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_multi_where_fixed;

-- Optimized: WHERE-fixed leading + BY tail
DROP TABLE IF EXISTS test_where_fixed_by_tail;
CREATE TABLE test_where_fixed_by_tail (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_where_fixed_by_tail SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_where_fixed_by_tail WHERE a = 1 LIMIT 5 BY b, c SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_where_fixed_by_tail WHERE a = 1 LIMIT 5 BY b, c SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_where_fixed_by_tail WHERE a = 1 LIMIT 5 BY b, c SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_where_fixed_by_tail;

-- Optimized: interleaved fixed + BY
DROP TABLE IF EXISTS test_interleaved_fixed_by;
CREATE TABLE test_interleaved_fixed_by (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_interleaved_fixed_by SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_interleaved_fixed_by WHERE b = 2 LIMIT 5 BY a, c SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_interleaved_fixed_by WHERE b = 2 LIMIT 5 BY a, c SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_interleaved_fixed_by WHERE b = 2 LIMIT 5 BY a, c SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_interleaved_fixed_by;

-- Optimized: injective-function-wrapped equality
DROP TABLE IF EXISTS test_injective_wrapped;
CREATE TABLE test_injective_wrapped (a UInt32, b UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO test_injective_wrapped SELECT number % 1000, number % 97, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, z FROM test_injective_wrapped WHERE toString(a) = '1' LIMIT 5 BY b SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, z FROM test_injective_wrapped WHERE toString(a) = '1' LIMIT 5 BY b SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, z FROM test_injective_wrapped WHERE toString(a) = '1' LIMIT 5 BY b SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_injective_wrapped;

-- Optimized: transitively-fixed function in sort key
DROP TABLE IF EXISTS test_transitively_fixed;
CREATE TABLE test_transitively_fixed (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY ((a + b), c);
INSERT INTO test_transitively_fixed SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_transitively_fixed WHERE a = 1 AND b = 2 LIMIT 5 BY c SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_transitively_fixed WHERE a = 1 AND b = 2 LIMIT 5 BY c SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_transitively_fixed WHERE a = 1 AND b = 2 LIMIT 5 BY c SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_transitively_fixed;

-- Optimized: multi-step monotonic chain
DROP TABLE IF EXISTS test_monotonic_chain;
CREATE TABLE test_monotonic_chain (a UInt32, b UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO test_monotonic_chain SELECT number % 1000, number % 97, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, z FROM test_monotonic_chain LIMIT 5 BY a, intDiv(b, 100) SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, z FROM test_monotonic_chain LIMIT 5 BY a, intDiv(b, 100) SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, z FROM test_monotonic_chain LIMIT 5 BY a, intDiv(b, 100) SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_monotonic_chain;

-- Through CreatingSetsStep / IN (subquery): findReadingStep crosses CreatingSetsStep
DROP TABLE IF EXISTS test_creating_sets;
DROP TABLE IF EXISTS test_creating_sets_in;
CREATE TABLE test_creating_sets (a UInt32, b UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b);
CREATE TABLE test_creating_sets_in (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO test_creating_sets SELECT number % 1000, number % 97, number FROM numbers_mt(1e5);
INSERT INTO test_creating_sets_in SELECT number FROM numbers_mt(10);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, z FROM test_creating_sets WHERE a IN (SELECT x FROM test_creating_sets_in) LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, z FROM test_creating_sets WHERE a IN (SELECT x FROM test_creating_sets_in) LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, z FROM test_creating_sets WHERE a IN (SELECT x FROM test_creating_sets_in) LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_creating_sets;
DROP TABLE test_creating_sets_in;

-- Through ArrayJoinStep: findReadingStep crosses ARRAY JOIN
DROP TABLE IF EXISTS test_array_join;
CREATE TABLE test_array_join (a UInt32, arr Array(UInt32)) ENGINE = MergeTree ORDER BY a;
INSERT INTO test_array_join SELECT number % 100, [1, 2, 3] FROM numbers_mt(1e3);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, x FROM test_array_join ARRAY JOIN arr AS x LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, x FROM test_array_join ARRAY JOIN arr AS x LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, x FROM test_array_join ARRAY JOIN arr AS x LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_array_join;

-- Optimized through a Merge engine: both children share the sort key and `a` is a PK prefix.
DROP TABLE IF EXISTS test_merge_part_1;
DROP TABLE IF EXISTS test_merge_part_2;
DROP TABLE IF EXISTS test_merge_wrap;
CREATE TABLE test_merge_part_1 (a UInt32, b UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b);
CREATE TABLE test_merge_part_2 (a UInt32, b UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO test_merge_part_1 SELECT number % 500, number % 97, number FROM numbers_mt(5e4);
INSERT INTO test_merge_part_2 SELECT number % 500 + 500, number % 97, number FROM numbers_mt(5e4);
CREATE TABLE test_merge_wrap AS test_merge_part_1 ENGINE = Merge(currentDatabase(), '^test_merge_part_[12]$');
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, z FROM test_merge_wrap LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, z FROM test_merge_wrap LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, z FROM test_merge_wrap LIMIT 5 BY a SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_merge_wrap;
DROP TABLE test_merge_part_1;
DROP TABLE test_merge_part_2;

-- Not optimized: Merge engine where the BY column is not a PK prefix in either child.
-- Negative-case guard that the optimization correctly declines to fire through Merge.
DROP TABLE IF EXISTS test_merge_nonpk_part_1;
DROP TABLE IF EXISTS test_merge_nonpk_part_2;
DROP TABLE IF EXISTS test_merge_nonpk_wrap;
CREATE TABLE test_merge_nonpk_part_1 (a UInt32, b UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b);
CREATE TABLE test_merge_nonpk_part_2 (a UInt32, b UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO test_merge_nonpk_part_1 SELECT number % 500, number % 97, number FROM numbers_mt(5e4);
INSERT INTO test_merge_nonpk_part_2 SELECT number % 500, number % 97, number + 1000 FROM numbers_mt(5e4);
CREATE TABLE test_merge_nonpk_wrap AS test_merge_nonpk_part_1 ENGINE = Merge(currentDatabase(), '^test_merge_nonpk_part_[12]$');
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, z FROM test_merge_nonpk_wrap LIMIT 5 BY z SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, z FROM test_merge_nonpk_wrap LIMIT 5 BY z SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, z FROM test_merge_nonpk_wrap LIMIT 5 BY z SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_merge_nonpk_wrap;
DROP TABLE test_merge_nonpk_part_1;
DROP TABLE test_merge_nonpk_part_2;

-- Optimized: multi-AND filter with mix of fixing and irrelevant conjuncts
DROP TABLE IF EXISTS test_multi_and;
CREATE TABLE test_multi_and (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
INSERT INTO test_multi_and SELECT number % 1000, number % 97, number % 7, number FROM numbers_mt(1e5);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\1') FROM (EXPLAIN PIPELINE SELECT a, b, c, z FROM test_multi_and WHERE a = 1 AND z > 0 AND b = 2 LIMIT 5 BY c SETTINGS optimize_limit_by_in_order = 1) WHERE explain LIKE '%LimitBy%Transform%';
SELECT (SELECT count() FROM (SELECT a, b, c, z FROM test_multi_and WHERE a = 1 AND z > 0 AND b = 2 LIMIT 5 BY c SETTINGS optimize_limit_by_in_order = 0)) = (SELECT count() FROM (SELECT a, b, c, z FROM test_multi_and WHERE a = 1 AND z > 0 AND b = 2 LIMIT 5 BY c SETTINGS optimize_limit_by_in_order = 1));
DROP TABLE test_multi_and;

-- An outer LIMIT above LIMIT BY bounds how many groups each sorted stream must produce, so a stream stops
-- after that many complete groups. Issue #113110.
DROP TABLE IF EXISTS test_outer_limit;
CREATE TABLE test_outer_limit (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
-- Eight parts, so that the read has several streams at any thread count: one stream serves the outer
-- LIMIT from the first granules whether or not the group limit applies.
SYSTEM STOP MERGES test_outer_limit;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(0, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(125000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(250000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(375000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(500000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(625000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(750000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(875000, 125000) SETTINGS max_insert_block_size = 1000000;

-- The reported shape: reverse read, LIMIT BY on the full key. Reads under a tenth of the table.
SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a, b, c) LIMIT 10 SETTINGS log_comment = '04238_outer_limit_full';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows < 100000 FROM system.query_log WHERE log_comment = '04238_outer_limit_full' AND current_database = currentDatabase() AND type = 'QueryFinish';

-- The group limit applies.
SELECT extract(explain, 'Per-stream LIMIT BY.*') FROM (EXPLAIN actions = 1 SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a, b, c) LIMIT 10) WHERE explain LIKE '%Per-stream LIMIT BY%';
SELECT explain LIKE '%"Per-stream LIMIT BY Max Groups": 10%' FROM (EXPLAIN json = 1, actions = 1 SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a, b, c) LIMIT 10);
SELECT extract(explain, 'Per-stream LIMIT BY.*') FROM (EXPLAIN actions = 1 SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a) LIMIT 10) WHERE explain LIKE '%Per-stream LIMIT BY%';
SELECT extract(explain, 'Per-stream LIMIT BY.*') FROM (EXPLAIN actions = 1 SELECT a, b, c, z FROM test_outer_limit ORDER BY a, b, c LIMIT 1 BY (a, b, c) LIMIT 10) WHERE explain LIKE '%Per-stream LIMIT BY%';
SELECT extract(explain, 'Per-stream LIMIT BY.*') FROM (EXPLAIN actions = 1 SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a) LIMIT 10 OFFSET 5) WHERE explain LIKE '%Per-stream LIMIT BY%';
SELECT extract(explain, 'Per-stream LIMIT BY.*') FROM (EXPLAIN actions = 1 SELECT * FROM (SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a, b, c)) LIMIT 10) WHERE explain LIKE '%Per-stream LIMIT BY%';

-- The group limit does not apply; the LIMIT BY is still pushed into the sort.
-- A LIMIT BY OFFSET can drop whole groups, so no number of groups is enough.
SELECT extract(explain, 'Per-stream LIMIT BY.*') FROM (EXPLAIN actions = 1 SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 2 OFFSET 1 BY (a) LIMIT 10) WHERE explain LIKE '%Per-stream LIMIT BY%';
-- No LIMIT, or one that can need rows of more groups.
SELECT extract(explain, 'Per-stream LIMIT BY.*') FROM (EXPLAIN actions = 1 SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a, b, c)) WHERE explain LIKE '%Per-stream LIMIT BY%';
SELECT extract(explain, 'Per-stream LIMIT BY.*') FROM (EXPLAIN actions = 1 SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a, b, c) LIMIT 10 WITH TIES) WHERE explain LIKE '%Per-stream LIMIT BY%';
-- The input must be read to the end.
SELECT extract(explain, 'Per-stream LIMIT BY.*') FROM (EXPLAIN actions = 1 SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a, b, c) LIMIT 10 SETTINGS exact_rows_before_limit = 1) WHERE explain LIKE '%Per-stream LIMIT BY%';
SELECT extract(explain, 'Per-stream LIMIT BY.*') FROM (EXPLAIN actions = 1 SELECT a, count() AS n FROM test_outer_limit GROUP BY a WITH TOTALS ORDER BY a DESC LIMIT 1 BY (a) LIMIT 10) WHERE explain LIKE '%Per-stream LIMIT BY%';
SELECT extract(explain, 'Per-stream LIMIT BY.*') FROM (EXPLAIN actions = 1 SELECT a, n FROM (SELECT a, count() AS n FROM test_outer_limit GROUP BY a WITH TOTALS) ORDER BY a DESC LIMIT 1 BY (a) LIMIT 10) WHERE explain LIKE '%Per-stream LIMIT BY%';
-- Disabled by the setting.
SELECT extract(explain, 'Per-stream LIMIT BY.*') FROM (EXPLAIN actions = 1 SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a, b, c) LIMIT 10 SETTINGS query_plan_limit_by_use_outer_limit = 0) WHERE explain LIKE '%Per-stream LIMIT BY%';
DROP TABLE test_outer_limit;

-- A group whose rows span a chunk boundary is still counted once, and one spanning several parts is
-- counted per stream, so the final single-stream LIMIT BY still sees every row it needs.
DROP TABLE IF EXISTS test_outer_limit_straddle;
CREATE TABLE test_outer_limit_straddle (a UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, z);
INSERT INTO test_outer_limit_straddle SELECT intDiv(number, 7), number FROM numbers_mt(70000);
SELECT a, z FROM test_outer_limit_straddle ORDER BY a DESC, z DESC LIMIT 3 BY (a) LIMIT 9 SETTINGS max_block_size = 5;
DROP TABLE test_outer_limit_straddle;

DROP TABLE IF EXISTS test_outer_limit_parts;
CREATE TABLE test_outer_limit_parts (a UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, z);
SYSTEM STOP MERGES test_outer_limit_parts;
INSERT INTO test_outer_limit_parts SELECT number % 5, number FROM numbers_mt(30000);
INSERT INTO test_outer_limit_parts SELECT number % 5, number + 100000 FROM numbers_mt(30000);
INSERT INTO test_outer_limit_parts SELECT number % 5, number + 200000 FROM numbers_mt(30000);
SELECT a, z FROM test_outer_limit_parts ORDER BY a DESC, z DESC LIMIT 3 BY (a) LIMIT 9;
DROP TABLE test_outer_limit_parts;

-- Totals are computed over the whole input.
DROP TABLE IF EXISTS test_outer_limit_totals;
CREATE TABLE test_outer_limit_totals (a UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, z);
INSERT INTO test_outer_limit_totals SELECT number % 5000, number FROM numbers_mt(200000);
SELECT a, count() AS c FROM test_outer_limit_totals GROUP BY a WITH TOTALS ORDER BY a DESC LIMIT 1 BY (a) LIMIT 10;
DROP TABLE test_outer_limit_totals;
