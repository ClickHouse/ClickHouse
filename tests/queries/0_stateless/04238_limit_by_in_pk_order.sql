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

-- An outer LIMIT above LIMIT BY bounds how many groups the read must produce, so each stream may stop
-- after that many completed groups. Issue #113110.
DROP TABLE IF EXISTS test_outer_limit;
CREATE TABLE test_outer_limit (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
-- Eight parts, not one. With a single part `num_streams` collapses to 1, and one stream serves the
-- outer LIMIT from the first granules whether or not a group bound was recorded, so the `read_rows`
-- assertions below stop discriminating. Eight parts keep them meaningful at any stream count.
SYSTEM STOP MERGES test_outer_limit;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(0, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(125000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(250000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(375000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(500000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(625000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(750000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(875000, 125000) SETTINGS max_insert_block_size = 1000000;

-- Reverse read, LIMIT BY on the full key: the reported shape. Reads under a tenth of the table.
SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a, b, c) LIMIT 10 SETTINGS log_comment = '04238_outer_limit_full';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows < 100000 FROM system.query_log WHERE log_comment = '04238_outer_limit_full' AND current_database = currentDatabase() AND type = 'QueryFinish';

-- LIMIT BY on a strict key prefix.
SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a) LIMIT 10 SETTINGS log_comment = '04238_outer_limit_prefix';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows < 100000 FROM system.query_log WHERE log_comment = '04238_outer_limit_prefix' AND current_database = currentDatabase() AND type = 'QueryFinish';

-- Forward read.
SELECT a, b, c, z FROM test_outer_limit ORDER BY a ASC, b ASC, c ASC LIMIT 1 BY (a, b, c) LIMIT 10 SETTINGS log_comment = '04238_outer_limit_asc';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows < 500000 FROM system.query_log WHERE log_comment = '04238_outer_limit_asc' AND current_database = currentDatabase() AND type = 'QueryFinish';

-- Not optimized: the LIMIT BY key is not a sort prefix, so groups are not contiguous and no group count
-- bounds the read. The hash variant runs. `c` has only 7 distinct values, so 7 groups can never satisfy
-- the outer LIMIT 10 and the whole table is read regardless of thread count. Do not raise the
-- cardinality of this key: that is what keeps the assertion below sound.
SELECT count() FROM (SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (c) LIMIT 10) SETTINGS log_comment = '04238_outer_limit_nonkey';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows > 500000 FROM system.query_log WHERE log_comment = '04238_outer_limit_nonkey' AND current_database = currentDatabase() AND type = 'QueryFinish';

-- Not optimized: with a LIMIT BY OFFSET a group can yield no output row at all, so the number of groups
-- an outer LIMIT needs is unbounded. Results stay exact and the read is not bounded.
SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 2 OFFSET 1 BY (a) LIMIT 10 SETTINGS log_comment = '04238_outer_limit_offset';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows > 500000 FROM system.query_log WHERE log_comment = '04238_outer_limit_offset' AND current_database = currentDatabase() AND type = 'QueryFinish';

-- Not optimized: no outer LIMIT, so nothing bounds the number of groups.
SELECT count() FROM (SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a, b, c)) SETTINGS log_comment = '04238_outer_limit_absent';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows > 500000 FROM system.query_log WHERE log_comment = '04238_outer_limit_absent' AND current_database = currentDatabase() AND type = 'QueryFinish';

-- Not optimized: `exact_rows_before_limit` requires reading to the end to report an exact count, which an
-- early stop would truncate.
SELECT a, b, c, z FROM test_outer_limit ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a, b, c) LIMIT 10 SETTINGS exact_rows_before_limit = 1, log_comment = '04238_outer_limit_exact_rows';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows > 500000 FROM system.query_log WHERE log_comment = '04238_outer_limit_exact_rows' AND current_database = currentDatabase() AND type = 'QueryFinish';
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

-- A stateful function under the LIMIT BY sees a different set of rows if a stream stops early, so the
-- group bound is not recorded when one is present anywhere in the subtree.
DROP TABLE IF EXISTS test_outer_limit_stateful;
CREATE TABLE test_outer_limit_stateful (a UInt32, b UInt32, c UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, b, c);
SYSTEM STOP MERGES test_outer_limit_stateful;
INSERT INTO test_outer_limit_stateful SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(0, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit_stateful SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(125000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit_stateful SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(250000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit_stateful SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(375000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit_stateful SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(500000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit_stateful SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(625000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit_stateful SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(750000, 125000) SETTINGS max_insert_block_size = 1000000;
INSERT INTO test_outer_limit_stateful SELECT number % 100000, number % 97, number % 7, number FROM numbers_mt(875000, 125000) SETTINGS max_insert_block_size = 1000000;
SELECT a, b, c, z, neighbor(z, 1) AS nb FROM test_outer_limit_stateful ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a, b, c) LIMIT 10 SETTINGS allow_deprecated_error_prone_window_functions = 1, log_comment = '04238_outer_limit_stateful_expr';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows > 500000 FROM system.query_log WHERE log_comment = '04238_outer_limit_stateful_expr' AND current_database = currentDatabase() AND type = 'QueryFinish';
SELECT a, b, c, z FROM test_outer_limit_stateful PREWHERE neighbor(z, 1) >= 0 ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a, b, c) LIMIT 10 SETTINGS allow_deprecated_error_prone_window_functions = 1, log_comment = '04238_outer_limit_stateful_prewhere';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows > 500000 FROM system.query_log WHERE log_comment = '04238_outer_limit_stateful_prewhere' AND current_database = currentDatabase() AND type = 'QueryFinish';
SELECT a, b, c, z FROM (SELECT a, b, c, z, neighbor(z, 1) AS nb FROM test_outer_limit_stateful ORDER BY a DESC, b DESC, c DESC) WHERE nb >= 0 ORDER BY a DESC, b DESC, c DESC LIMIT 1 BY (a, b, c) LIMIT 10 SETTINGS allow_deprecated_error_prone_window_functions = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, log_comment = '04238_outer_limit_stateful_filter';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows > 500000 FROM system.query_log WHERE log_comment = '04238_outer_limit_stateful_filter' AND current_database = currentDatabase() AND type = 'QueryFinish';
DROP TABLE test_outer_limit_stateful;

-- `WITH TOTALS` together with an `ORDER BY` leaves `always_read_till_end` unset, and totals are emitted
-- only once the input finishes, so an early stop would report partial totals.
DROP TABLE IF EXISTS test_outer_limit_totals;
CREATE TABLE test_outer_limit_totals (a UInt32, z UInt32) ENGINE = MergeTree ORDER BY (a, z);
-- Many more groups than the outer LIMIT, so a group bound of 10 would really stop the read: with only
-- 10 groups the stop is unreachable and the case cannot detect a missing guard.
INSERT INTO test_outer_limit_totals SELECT number % 5000, number FROM numbers_mt(200000);
SELECT a, count() AS c FROM test_outer_limit_totals GROUP BY a WITH TOTALS ORDER BY a DESC LIMIT 1 BY (a) LIMIT 10;
DROP TABLE test_outer_limit_totals;
