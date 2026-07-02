SET optimize_read_in_order = 1, optimize_sorting_by_input_stream_properties = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET enable_vertical_final = 0;
SET optimize_on_insert = 1;
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Nested(c1 Int)) ENGINE = SummingMergeTree ORDER BY (c0.c1 DESC) SETTINGS allow_experimental_reverse_key = 1;
INSERT INTO t0 (c0.c1) VALUES ([1]), ([2]);
SELECT 1 FROM t0 FINAL;
DROP TABLE t0;

-- For consistency of the EXPLAIN output:
SET allow_prefetched_read_pool_for_remote_filesystem = 0;

-- PartsSplitter should work for reverse keys.
CREATE TABLE t0(a Int, b Int) Engine=ReplacingMergeTree order by (a desc, b desc) SETTINGS allow_experimental_reverse_key = 1, allow_nullable_key = 1, index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t0 select number, number from numbers(5);
INSERT INTO t0 select number, number from numbers(5, 2);
set max_threads = 2;
explain pipeline select * from t0 final SETTINGS enable_vertical_final = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
DROP TABLE t0;

-- PartsSplitter is disabled when some keys are in ascending order while others are in descending order.
CREATE TABLE t0(a Int, b Int) Engine=ReplacingMergeTree order by (a desc, b) SETTINGS allow_experimental_reverse_key = 1, index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t0 select number, number from numbers(5);
INSERT INTO t0 select number, number from numbers(5,2);
set max_threads = 2;
explain pipeline select * from t0 final SETTINGS enable_vertical_final = 0;
DROP TABLE t0;
