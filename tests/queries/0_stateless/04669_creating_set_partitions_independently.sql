-- Tags: long, no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: Explain output may differ

-- Unlike DISTINCT / GROUP BY, per-partition set building has no cost heuristic (the ordinary set fill is
-- single-stream, so any parallelism helps); it only requires more than one partition. max_threads is
-- pinned so that the `DistinctTransform × N` pipeline assertion below is stable.
SET max_threads = 8;
-- The optimization is disabled under parallel replicas.
SET enable_parallel_replicas = 0;
-- The abandon cases below need every stream to cross the observation window of the preliminary
-- deduplication (a fixed number of chunks, see `DeduplicationAbandonController`). A block size below
-- the index granularity makes every read chunk a single granule of 8192 rows, shrinking the window
-- about eightfold: with 8 streams it is crossed within a few hundred thousand rows instead of a few
-- million.
SET max_block_size = 6540;

-- The pretty EXPLAIN output decorates plan lines with tree-drawing characters; use the legacy format
-- so the assertions below match plain `Pre-distinct: 1` lines.
SET explain_query_plan_default = 'legacy';

-- { echo }

-- partition key equals the subquery output column
DROP TABLE IF EXISTS test_in_partition_eq_key;
CREATE TABLE test_in_partition_eq_key (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_in_partition_eq_key;
INSERT INTO test_in_partition_eq_key SELECT number % 64, number FROM numbers_mt(400);
INSERT INTO test_in_partition_eq_key SELECT number % 64, number FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_partition_eq_key) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_partition_eq_key) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_partition_eq_key) SETTINGS allow_creating_set_partitions_independently = 1);
-- the parallel pre-deduplication is visible in the pipeline: one DistinctTransform per partition port
-- feeding the single set-filling transform
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_partition_eq_key) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%DistinctTransform%' OR explain LIKE '%CreatingSetsTransform%';
-- set size limits are enforced by the single filling transform against the unique row count, so a limit
-- equal to the number of unique keys does not fire even though many more (duplicated) rows are read
SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_partition_eq_key) SETTINGS allow_creating_set_partitions_independently = 1, max_rows_in_set = 64, set_overflow_mode = 'throw';
SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_partition_eq_key) SETTINGS allow_creating_set_partitions_independently = 1, max_rows_in_set = 63, set_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
-- NEGATIVE: optimization disabled
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_partition_eq_key) SETTINGS allow_creating_set_partitions_independently = 0) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_in_partition_eq_key;

-- WHERE clause in the subquery (filter folded into the read)
DROP TABLE IF EXISTS test_in_with_where_filter;
CREATE TABLE test_in_with_where_filter (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY b PARTITION BY a % 8;
SYSTEM STOP MERGES test_in_with_where_filter;
INSERT INTO test_in_with_where_filter SELECT number % 64, number FROM numbers_mt(400);
INSERT INTO test_in_with_where_filter SELECT number % 64, number + 1 FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_with_where_filter WHERE b > 0) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_with_where_filter WHERE b > 0) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_with_where_filter WHERE b > 0) SETTINGS allow_creating_set_partitions_independently = 1);
-- explicit Filter step (PREWHERE disabled)
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_with_where_filter WHERE b > 0) SETTINGS allow_creating_set_partitions_independently = 1, optimize_move_to_prewhere = 0) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_in_with_where_filter;

-- partition key is a function of the subquery output column
DROP TABLE IF EXISTS test_in_partition_func_of_key;
CREATE TABLE test_in_partition_func_of_key (d Date, x UInt32) ENGINE = MergeTree ORDER BY d PARTITION BY toYYYYMM(d);
SYSTEM STOP MERGES test_in_partition_func_of_key;
INSERT INTO test_in_partition_func_of_key SELECT toDate('2024-01-01') + (number % 240), number FROM numbers_mt(480);
INSERT INTO test_in_partition_func_of_key SELECT toDate('2024-01-01') + (number % 240), number + 1 FROM numbers_mt(480);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE toDate('2024-01-01') + number IN (SELECT d FROM test_in_partition_func_of_key) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM numbers(100) WHERE toDate('2024-01-01') + number IN (SELECT d FROM test_in_partition_func_of_key) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE toDate('2024-01-01') + number IN (SELECT d FROM test_in_partition_func_of_key) SETTINGS allow_creating_set_partitions_independently = 1);
DROP TABLE test_in_partition_func_of_key;

-- multi-column set, multi-column partition fully covered by the subquery output
DROP TABLE IF EXISTS test_in_multi_col_partition;
CREATE TABLE test_in_multi_col_partition (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY (intDiv(a, 2), intDiv(b, 3));
INSERT INTO test_in_multi_col_partition SELECT number, number FROM numbers_mt(24);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE (number, number) IN (SELECT a, b FROM test_in_multi_col_partition) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM numbers(100) WHERE (number, number) IN (SELECT a, b FROM test_in_multi_col_partition) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE (number, number) IN (SELECT a, b FROM test_in_multi_col_partition) SETTINGS allow_creating_set_partitions_independently = 1);
DROP TABLE test_in_multi_col_partition;

-- injective function of the partition column as the subquery output
DROP TABLE IF EXISTS test_in_injective_wrapper;
CREATE TABLE test_in_injective_wrapper (user_id UInt32, payload String) ENGINE = MergeTree ORDER BY tuple() PARTITION BY user_id % 8;
INSERT INTO test_in_injective_wrapper SELECT number % 64, toString(number) FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE toString(number) IN (SELECT toString(user_id) FROM test_in_injective_wrapper) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM numbers(100) WHERE toString(number) IN (SELECT toString(user_id) FROM test_in_injective_wrapper) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE toString(number) IN (SELECT toString(user_id) FROM test_in_injective_wrapper) SETTINGS allow_creating_set_partitions_independently = 1);
DROP TABLE test_in_injective_wrapper;

-- ARRAY JOIN is a transparent (per-stream) step in the subquery
DROP TABLE IF EXISTS test_in_array_join;
CREATE TABLE test_in_array_join (a UInt32, arr Array(UInt32)) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_in_array_join;
INSERT INTO test_in_array_join SELECT number % 64, range(number % 5) FROM numbers_mt(400);
INSERT INTO test_in_array_join SELECT number % 64, range(number % 5) FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_array_join ARRAY JOIN arr) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_array_join ARRAY JOIN arr) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_array_join ARRAY JOIN arr) SETTINGS allow_creating_set_partitions_independently = 1);
-- NEGATIVE: the subquery outputs only the array-joined column (not partition-determined)
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT arr FROM test_in_array_join ARRAY JOIN arr) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_in_array_join;

-- disjointness propagation: a LIMIT BY that skips stream merging passes the property up to the set build
DROP TABLE IF EXISTS test_in_through_limit_by;
CREATE TABLE test_in_through_limit_by (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_in_through_limit_by;
INSERT INTO test_in_through_limit_by SELECT number % 64, number FROM numbers_mt(400);
INSERT INTO test_in_through_limit_by SELECT number % 64, number FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_through_limit_by LIMIT 2 BY a) SETTINGS allow_creating_set_partitions_independently = 1, allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_through_limit_by LIMIT 2 BY a) SETTINGS allow_creating_set_partitions_independently = 0, allow_limit_by_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_through_limit_by LIMIT 2 BY a) SETTINGS allow_creating_set_partitions_independently = 1, allow_limit_by_partitions_independently = 1);
-- NEGATIVE: without per-partition LIMIT BY the property never reaches the set build
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_through_limit_by LIMIT 2 BY a) SETTINGS allow_creating_set_partitions_independently = 1, allow_limit_by_partitions_independently = 0) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_in_through_limit_by;

-- disjointness propagation: a DISTINCT that skips stream merging passes the property up to the set build
DROP TABLE IF EXISTS test_in_through_distinct;
CREATE TABLE test_in_through_distinct (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_in_through_distinct;
INSERT INTO test_in_through_distinct SELECT number % 64, number FROM numbers_mt(400);
INSERT INTO test_in_through_distinct SELECT number % 64, number FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT DISTINCT a FROM test_in_through_distinct) SETTINGS allow_creating_set_partitions_independently = 1, allow_distinct_partitions_independently = 1, max_rows_in_distinct = 0, max_bytes_in_distinct = 0) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM numbers(100) WHERE number IN (SELECT DISTINCT a FROM test_in_through_distinct) SETTINGS allow_creating_set_partitions_independently = 0, allow_distinct_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE number IN (SELECT DISTINCT a FROM test_in_through_distinct) SETTINGS allow_creating_set_partitions_independently = 1, allow_distinct_partitions_independently = 1, max_rows_in_distinct = 0, max_bytes_in_distinct = 0);
DROP TABLE test_in_through_distinct;

-- NEGATIVE: the subquery output does not determine the partition
DROP TABLE IF EXISTS test_in_key_not_partition;
CREATE TABLE test_in_key_not_partition (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO test_in_key_not_partition SELECT number % 64, number FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT b FROM test_in_key_not_partition) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_in_key_not_partition;

-- NEGATIVE: only one partition (per-partition reading is pointless, so it is not requested)
DROP TABLE IF EXISTS test_in_single_partition;
CREATE TABLE test_in_single_partition (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_in_single_partition;
INSERT INTO test_in_single_partition SELECT 8 * (number % 25) FROM numbers_mt(200);
INSERT INTO test_in_single_partition SELECT 8 * (number % 25) FROM numbers_mt(200);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_single_partition) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_single_partition) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_single_partition) SETTINGS allow_creating_set_partitions_independently = 1);
DROP TABLE test_in_single_partition;

-- NEGATIVE: no PARTITION BY
DROP TABLE IF EXISTS test_in_no_partition_by;
CREATE TABLE test_in_no_partition_by (a UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO test_in_no_partition_by SELECT number % 64 FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_no_partition_by) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_in_no_partition_by;

-- NEGATIVE: FINAL in the subquery
DROP TABLE IF EXISTS test_in_with_final;
CREATE TABLE test_in_with_final (a UInt32, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY a PARTITION BY a % 8;
INSERT INTO test_in_with_final SELECT number % 64, 1 FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_with_final FINAL) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_in_with_final;

-- NEGATIVE: GROUP BY in the subquery. Aggregation consumes the disjointness (it is a barrier in the
-- propagation), so no pre-distinct is added, even though the aggregation itself may run per-partition;
-- filter only the pre-distinct marker to keep the output independent of the aggregation heuristic.
DROP TABLE IF EXISTS test_in_with_group_by;
CREATE TABLE test_in_with_group_by (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO test_in_with_group_by SELECT number % 64, number FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_with_group_by GROUP BY a) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%';
DROP TABLE test_in_with_group_by;

-- Two balanced partitions are enough: the skew check compares the largest partition against the average,
-- so a balanced layout passes at any partition count (halving the serial reduction already wins).
DROP TABLE IF EXISTS test_in_two_partitions;
CREATE TABLE test_in_two_partitions (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 2;
SYSTEM STOP MERGES test_in_two_partitions;
INSERT INTO test_in_two_partitions SELECT number % 64 FROM numbers_mt(400);
INSERT INTO test_in_two_partitions SELECT number % 64 FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_two_partitions) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_two_partitions) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_two_partitions) SETTINGS allow_creating_set_partitions_independently = 1);
DROP TABLE test_in_two_partitions;

-- NEGATIVE: heavily skewed partitions (the largest holds more than twice the rows of the average
-- partition). Deduplicating the dominant partition would run in a single stream, so the skew check
-- declines; force_creating_set_partitions_independently bypasses only that check.
DROP TABLE IF EXISTS test_in_skewed_partitions;
CREATE TABLE test_in_skewed_partitions (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY if(a % 10 < 9, 0, 1 + a % 15);
INSERT INTO test_in_skewed_partitions SELECT number % 64 FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_skewed_partitions) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_skewed_partitions) SETTINGS allow_creating_set_partitions_independently = 1, force_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_skewed_partitions) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_skewed_partitions) SETTINGS allow_creating_set_partitions_independently = 1, force_creating_set_partitions_independently = 1);
DROP TABLE test_in_skewed_partitions;

-- Nullable key: with the default transform_null_in = 0 the set fill skips rows with a NULL key, and the
-- preliminary deduplication drops them the same way; with transform_null_in = 1 NULL is a regular set
-- element. The partition key maps every NULL into one partition, so the layout stays balanced only when
-- NULLs are few.
DROP TABLE IF EXISTS test_in_nullable_key;
CREATE TABLE test_in_nullable_key (a Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple() PARTITION BY coalesce(a, 0) % 8;
SYSTEM STOP MERGES test_in_nullable_key;
INSERT INTO test_in_nullable_key SELECT if(number % 16 = 0, NULL, number % 64) FROM numbers_mt(400);
INSERT INTO test_in_nullable_key SELECT if(number % 16 = 0, NULL, number % 64) FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_nullable_key) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_nullable_key) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_nullable_key) SETTINGS allow_creating_set_partitions_independently = 1);
SELECT (SELECT count() FROM (SELECT if(number % 16 = 0, NULL, number % 64) AS n FROM numbers(100)) WHERE n IN (SELECT a FROM test_in_nullable_key) SETTINGS transform_null_in = 1, allow_creating_set_partitions_independently = 0) = (SELECT count() FROM (SELECT if(number % 16 = 0, NULL, number % 64) AS n FROM numbers(100)) WHERE n IN (SELECT a FROM test_in_nullable_key) SETTINGS transform_null_in = 1, allow_creating_set_partitions_independently = 1);
-- a constant NULL key component makes every key contain a NULL, so the set fill drops every row and
-- the set is empty; the preliminary deduplication emits nothing and stops reading instead of
-- deduplicating the whole input
SELECT count() FROM numbers(100) WHERE (number, NULL) IN (SELECT coalesce(a, 0), CAST(NULL AS Nullable(UInt8)) FROM test_in_nullable_key) SETTINGS allow_creating_set_partitions_independently = 1;
SELECT (SELECT count() FROM numbers(100) WHERE (number, NULL) IN (SELECT coalesce(a, 0), CAST(NULL AS Nullable(UInt8)) FROM test_in_nullable_key) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE (number, NULL) IN (SELECT coalesce(a, 0), CAST(NULL AS Nullable(UInt8)) FROM test_in_nullable_key) SETTINGS allow_creating_set_partitions_independently = 1);
DROP TABLE test_in_nullable_key;

-- LowCardinality(Nullable) key: the set fill strips LowCardinality before it skips rows with a NULL
-- key, so the preliminary deduplication drops rows whose LowCardinality key is NULL the same way (its
-- accounting must see the same row population as the fill); with transform_null_in = 1 NULL is a
-- regular set element.
DROP TABLE IF EXISTS test_in_lc_nullable_key;
CREATE TABLE test_in_lc_nullable_key (a LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY tuple() PARTITION BY sipHash64(coalesce(a, '')) % 8;
SYSTEM STOP MERGES test_in_lc_nullable_key;
INSERT INTO test_in_lc_nullable_key SELECT if(number % 16 = 0, NULL, toString(number % 64)) FROM numbers_mt(400);
INSERT INTO test_in_lc_nullable_key SELECT if(number % 16 = 0, NULL, toString(number % 64)) FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(100) WHERE toString(number) IN (SELECT a FROM test_in_lc_nullable_key) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM numbers(100) WHERE toString(number) IN (SELECT a FROM test_in_lc_nullable_key) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(100) WHERE toString(number) IN (SELECT a FROM test_in_lc_nullable_key) SETTINGS allow_creating_set_partitions_independently = 1);
SELECT (SELECT count() FROM (SELECT if(number % 16 = 0, NULL, toString(number % 64)) AS n FROM numbers(100)) WHERE n IN (SELECT a FROM test_in_lc_nullable_key) SETTINGS transform_null_in = 1, allow_creating_set_partitions_independently = 0) = (SELECT count() FROM (SELECT if(number % 16 = 0, NULL, toString(number % 64)) AS n FROM numbers(100)) WHERE n IN (SELECT a FROM test_in_lc_nullable_key) SETTINGS transform_null_in = 1, allow_creating_set_partitions_independently = 1);
DROP TABLE test_in_lc_nullable_key;

-- NEGATIVE: the lazy FINAL optimization builds an internal primary-key set through the same
-- `CreatingSetStep`; the per-partition pre-deduplication is scoped to `IN (subquery)` set fills and must
-- stay out of it. The `LazyFinalKeyAnalysis` marker pins that the lazy FINAL plan actually formed.
DROP TABLE IF EXISTS test_in_lazy_final;
CREATE TABLE test_in_lazy_final (a UInt64, v UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY a PARTITION BY a % 8;
SYSTEM STOP MERGES test_in_lazy_final;
INSERT INTO test_in_lazy_final SELECT number % 64, number, 1 FROM numbers_mt(400);
INSERT INTO test_in_lazy_final SELECT number % 32, number, 2 FROM numbers_mt(200);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT sum(v) FROM test_in_lazy_final FINAL WHERE v != 3 SETTINGS allow_creating_set_partitions_independently = 1, query_plan_optimize_lazy_final = 1) WHERE explain LIKE '%LazyFinalKeyAnalysis%' OR explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE test_in_lazy_final;

-- NEGATIVE: GLOBAL IN fills an external temporary table alongside the set, so the optimization does not apply
DROP TABLE IF EXISTS test_in_global;
CREATE TABLE test_in_global (a UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO test_in_global SELECT number % 64 FROM numbers_mt(400);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM remote('127.0.0.{1,2}', system.numbers) WHERE number < 100 AND number GLOBAL IN (SELECT a FROM test_in_global) SETTINGS allow_creating_set_partitions_independently = 1) WHERE explain LIKE '%Pre-distinct%';
SELECT count() FROM remote('127.0.0.{1,2}', system.numbers) WHERE number < 100 AND number GLOBAL IN (SELECT a FROM test_in_global) SETTINGS allow_creating_set_partitions_independently = 1;
-- With the outer table read through `remote`, the external table reaches the set build only at
-- pipeline build time, after the plan optimization checked for it. The transfer limits count the raw
-- subquery rows (400 here, 64 unique), so a limit between the two must still fire: the
-- pre-deduplication must stay out of the external-table write.
DROP TABLE IF EXISTS test_in_global_probe;
CREATE TABLE test_in_global_probe (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO test_in_global_probe SELECT number FROM numbers(100);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), test_in_global_probe) WHERE k GLOBAL IN (SELECT a FROM test_in_global) SETTINGS allow_creating_set_partitions_independently = 1, prefer_localhost_replica = 0) WHERE explain LIKE '%Pre-distinct%' OR explain LIKE '%Read each partition through separate port%';
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), test_in_global_probe) WHERE k GLOBAL IN (SELECT a FROM test_in_global) SETTINGS allow_creating_set_partitions_independently = 1, prefer_localhost_replica = 0, max_rows_to_transfer = 100, transfer_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), test_in_global_probe) WHERE k GLOBAL IN (SELECT a FROM test_in_global) SETTINGS allow_creating_set_partitions_independently = 1, prefer_localhost_replica = 0;
DROP TABLE test_in_global_probe;
DROP TABLE test_in_global;

-- LowCardinality key with a fully duplicate prefix: chunks whose dictionary was already fully seen are
-- resolved by the LC mask without touching the hash table, and the abandon accounting must still see
-- them as duplicate rows; otherwise a unique tail would make the stream look mostly unique.
DROP TABLE IF EXISTS test_in_lc_dup_prefix;
CREATE TABLE test_in_lc_dup_prefix (s LowCardinality(String)) ENGINE = MergeTree ORDER BY s PARTITION BY cityHash64(s) % 8;
SYSTEM STOP MERGES test_in_lc_dup_prefix;
INSERT INTO test_in_lc_dup_prefix SELECT concat('dup_', toString(number % 512)) FROM numbers_mt(400000);
INSERT INTO test_in_lc_dup_prefix SELECT concat('uniq_', toString(number)) FROM numbers_mt(100000);
SELECT (SELECT count() FROM (SELECT concat('dup_', toString(number % 512)) AS s FROM numbers(1000)) WHERE s IN (SELECT s FROM test_in_lc_dup_prefix) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM (SELECT concat('dup_', toString(number % 512)) AS s FROM numbers(1000)) WHERE s IN (SELECT s FROM test_in_lc_dup_prefix) SETTINGS allow_creating_set_partitions_independently = 1);
-- the set stores exactly the unique keys
SELECT count() FROM (SELECT concat('dup_', toString(number % 8)) AS s FROM numbers(8)) WHERE s IN (SELECT s FROM test_in_lc_dup_prefix) SETTINGS allow_creating_set_partitions_independently = 1, max_rows_in_set = 100512, set_overflow_mode = 'throw';
DROP TABLE test_in_lc_dup_prefix;

-- Mostly-unique input: the preliminary per-stream deduplication observes the first chunks, sees that
-- almost every row survives, and abandons itself, dropping its hash table and passing the remaining
-- chunks through. The duplicates inserted second arrive in each stream after that point and reach the
-- set fill, which deduplicates them anyway.
DROP TABLE IF EXISTS test_in_mostly_unique;
CREATE TABLE test_in_mostly_unique (a UInt64) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_in_mostly_unique;
INSERT INTO test_in_mostly_unique SELECT number FROM numbers_mt(400000);
INSERT INTO test_in_mostly_unique SELECT number FROM numbers_mt(100);
SELECT (SELECT count() FROM numbers(400000) WHERE number IN (SELECT a FROM test_in_mostly_unique) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(400000) WHERE number IN (SELECT a FROM test_in_mostly_unique) SETTINGS allow_creating_set_partitions_independently = 1);
-- the set stores exactly the unique keys even though the pre-deduplication stopped removing rows
SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_mostly_unique) SETTINGS allow_creating_set_partitions_independently = 1, max_rows_in_set = 400000, set_overflow_mode = 'throw';
DROP TABLE test_in_mostly_unique;

-- A heavy duplicate head anchors the cumulative unique rate below the abandon threshold, so the
-- deduplication stays engaged through the unique tail, and the duplicates inserted last are still
-- removed by it.
DROP TABLE IF EXISTS test_in_dup_head_unique_tail;
CREATE TABLE test_in_dup_head_unique_tail (a UInt64) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_in_dup_head_unique_tail;
INSERT INTO test_in_dup_head_unique_tail SELECT number % 10000 FROM numbers_mt(320000);
INSERT INTO test_in_dup_head_unique_tail SELECT 1000000 + number FROM numbers_mt(1280000);
INSERT INTO test_in_dup_head_unique_tail SELECT number % 10000 FROM numbers_mt(100);
SELECT (SELECT count() FROM numbers(20000) WHERE number IN (SELECT a FROM test_in_dup_head_unique_tail) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(20000) WHERE number IN (SELECT a FROM test_in_dup_head_unique_tail) SETTINGS allow_creating_set_partitions_independently = 1);
SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_dup_head_unique_tail) SETTINGS allow_creating_set_partitions_independently = 1, max_rows_in_set = 1290000, set_overflow_mode = 'throw';
DROP TABLE test_in_dup_head_unique_tail;

-- Mostly-unique wide String keys: the observation retains every unique key it sees, so for wide keys
-- the byte cap ends the observation before the chunk window does and the deduplication abandons after
-- fewer chunks. The duplicates inserted second arrive after that point and reach the set fill, which
-- deduplicates them anyway.
DROP TABLE IF EXISTS test_in_wide_keys;
CREATE TABLE test_in_wide_keys (a String) ENGINE = MergeTree ORDER BY tuple() PARTITION BY sipHash64(a) % 8;
SYSTEM STOP MERGES test_in_wide_keys;
INSERT INTO test_in_wide_keys SELECT concat(toString(number), repeat('x', 700)) FROM numbers_mt(320000);
INSERT INTO test_in_wide_keys SELECT concat(toString(number), repeat('x', 700)) FROM numbers_mt(100);
SELECT (SELECT count() FROM numbers(320100) WHERE concat(toString(number), repeat('x', 700)) IN (SELECT a FROM test_in_wide_keys) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(320100) WHERE concat(toString(number), repeat('x', 700)) IN (SELECT a FROM test_in_wide_keys) SETTINGS allow_creating_set_partitions_independently = 1);
-- the set stores exactly the unique keys even though the pre-deduplication stopped removing rows
SELECT count() FROM numbers(100) WHERE concat(toString(number), repeat('x', 700)) IN (SELECT a FROM test_in_wide_keys) SETTINGS allow_creating_set_partitions_independently = 1, max_rows_in_set = 320000, set_overflow_mode = 'throw';
DROP TABLE test_in_wide_keys;

-- A small duplicate head: once the unique tail outweighs it, the cumulative unique rate crosses the
-- threshold and the deduplication abandons mid-stream. The duplicates inserted last arrive after that
-- point and reach the set fill, which deduplicates them anyway.
DROP TABLE IF EXISTS test_in_small_dup_head;
CREATE TABLE test_in_small_dup_head (a UInt64) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES test_in_small_dup_head;
INSERT INTO test_in_small_dup_head SELECT number % 1000 FROM numbers_mt(51200);
INSERT INTO test_in_small_dup_head SELECT 100000 + number FROM numbers_mt(640000);
INSERT INTO test_in_small_dup_head SELECT number % 1000 FROM numbers_mt(100);
SELECT (SELECT count() FROM numbers(200000) WHERE number IN (SELECT a FROM test_in_small_dup_head) SETTINGS allow_creating_set_partitions_independently = 0) = (SELECT count() FROM numbers(200000) WHERE number IN (SELECT a FROM test_in_small_dup_head) SETTINGS allow_creating_set_partitions_independently = 1);
-- the set stores exactly the unique keys even though the pre-deduplication stopped removing rows
SELECT count() FROM numbers(100) WHERE number IN (SELECT a FROM test_in_small_dup_head) SETTINGS allow_creating_set_partitions_independently = 1, max_rows_in_set = 641000, set_overflow_mode = 'throw';
DROP TABLE test_in_small_dup_head;
