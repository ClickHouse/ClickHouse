-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101838
-- read-in-order must treat LowCardinality(Float) sorting keys like plain Float keys,
-- so NaN (which sorts like NULL) is placed correctly under every NULLS direction.
-- Plain Nullable(Float64) and Nullable(Int) keys are covered too, since the guard
-- that disables read-in-order for the unsupported NULLS direction applies to them.
-- This covers the query-plan optimization (optimizeReadInOrder); the legacy
-- ReadInOrderOptimizer path (old analyzer, query_plan_read_in_order = 0) has a
-- separate, pre-existing reverse-key limitation and is intentionally not exercised here.

-- { echo }
SET optimize_read_in_order = 1;
SET max_threads = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS test_lc_float_nan;
CREATE TABLE test_lc_float_nan (c0 LowCardinality(Float64)) ENGINE = MergeTree() ORDER BY c0;
INSERT INTO test_lc_float_nan VALUES (0), (nan), (1);

DROP TABLE IF EXISTS test_lc_float32_nan;
CREATE TABLE test_lc_float32_nan (c0 LowCardinality(Float32)) ENGINE = MergeTree() ORDER BY c0;
INSERT INTO test_lc_float32_nan VALUES (0), (nan), (1);

DROP TABLE IF EXISTS test_lc_bfloat16_nan;
CREATE TABLE test_lc_bfloat16_nan (c0 LowCardinality(BFloat16)) ENGINE = MergeTree() ORDER BY c0;
INSERT INTO test_lc_bfloat16_nan VALUES (0), (nan), (1);

DROP TABLE IF EXISTS test_nullable_float_nan;
CREATE TABLE test_nullable_float_nan (c0 Nullable(Float64)) ENGINE = MergeTree() ORDER BY c0 SETTINGS allow_nullable_key = 1;
INSERT INTO test_nullable_float_nan VALUES (0), (nan), (1), (NULL);

DROP TABLE IF EXISTS test_nullable_int;
CREATE TABLE test_nullable_int (c0 Nullable(Int32)) ENGINE = MergeTree() ORDER BY c0 SETTINGS allow_nullable_key = 1;
INSERT INTO test_nullable_int VALUES (0), (1), (NULL);

-- Query-plan read-in-order path (analyzer).
SET enable_analyzer = 1;
SET query_plan_read_in_order = 1;

SELECT * FROM test_lc_float_nan ORDER BY c0 ASC NULLS FIRST;
SELECT * FROM test_lc_float_nan ORDER BY c0 ASC NULLS LAST;
SELECT * FROM test_lc_float_nan ORDER BY c0 DESC NULLS FIRST;
SELECT * FROM test_lc_float_nan ORDER BY c0 DESC NULLS LAST;

SELECT * FROM test_lc_float32_nan ORDER BY c0 ASC NULLS FIRST;
SELECT * FROM test_lc_float32_nan ORDER BY c0 ASC NULLS LAST;
SELECT * FROM test_lc_float32_nan ORDER BY c0 DESC NULLS FIRST;
SELECT * FROM test_lc_float32_nan ORDER BY c0 DESC NULLS LAST;

SELECT * FROM test_lc_bfloat16_nan ORDER BY c0 ASC NULLS FIRST;
SELECT * FROM test_lc_bfloat16_nan ORDER BY c0 ASC NULLS LAST;
SELECT * FROM test_lc_bfloat16_nan ORDER BY c0 DESC NULLS FIRST;
SELECT * FROM test_lc_bfloat16_nan ORDER BY c0 DESC NULLS LAST;

SELECT * FROM test_nullable_float_nan ORDER BY c0 ASC NULLS FIRST;
SELECT * FROM test_nullable_float_nan ORDER BY c0 ASC NULLS LAST;
SELECT * FROM test_nullable_float_nan ORDER BY c0 DESC NULLS FIRST;
SELECT * FROM test_nullable_float_nan ORDER BY c0 DESC NULLS LAST;

SELECT * FROM test_nullable_int ORDER BY c0 ASC NULLS FIRST;
SELECT * FROM test_nullable_int ORDER BY c0 ASC NULLS LAST;
SELECT * FROM test_nullable_int ORDER BY c0 DESC NULLS FIRST;
SELECT * FROM test_nullable_int ORDER BY c0 DESC NULLS LAST;

-- Monotonic reversing expression: negate is strictly decreasing, so it flips the key order.
-- NaN/NULL still sort as the largest value, so the side they land on flips with it.
-- Read-in-order must honor the requested NULLS direction even through such a function.
-- Regression for https://github.com/ClickHouse/ClickHouse/pull/106588#discussion_r3367031538
SELECT * FROM test_lc_float_nan ORDER BY negate(c0) ASC NULLS FIRST;
SELECT * FROM test_lc_float_nan ORDER BY negate(c0) ASC NULLS LAST;
SELECT * FROM test_lc_float_nan ORDER BY negate(c0) DESC NULLS FIRST;
SELECT * FROM test_lc_float_nan ORDER BY negate(c0) DESC NULLS LAST;
SELECT * FROM test_nullable_float_nan ORDER BY negate(c0) ASC NULLS FIRST;
SELECT * FROM test_nullable_float_nan ORDER BY negate(c0) ASC NULLS LAST;
SELECT * FROM test_nullable_float_nan ORDER BY negate(c0) DESC NULLS FIRST;
SELECT * FROM test_nullable_float_nan ORDER BY negate(c0) DESC NULLS LAST;

-- A leading NaN/NULL-capable key fixed by WHERE has a single value, so the NULLS direction
-- requested for it (or the one of the next ORDER BY column) must not disable read-in-order:
-- the read direction comes from the next key column.
-- Regression for https://github.com/ClickHouse/ClickHouse/pull/106588#discussion_r4144175524
DROP TABLE IF EXISTS test_lc_float_fixed;
CREATE TABLE test_lc_float_fixed (k LowCardinality(Float64), ts UInt32) ENGINE = MergeTree() ORDER BY (k, ts);
INSERT INTO test_lc_float_fixed SELECT arrayElement([0, 1, nan], number % 3 + 1), number FROM numbers(12);
DROP TABLE IF EXISTS test_nullable_float_fixed;
CREATE TABLE test_nullable_float_fixed (k Nullable(Float64), ts UInt32) ENGINE = MergeTree() ORDER BY (k, ts) SETTINGS allow_nullable_key = 1;
INSERT INTO test_nullable_float_fixed SELECT arrayElement([0, 1, nan, NULL], number % 4 + 1), number FROM numbers(12);

SELECT ts FROM test_lc_float_fixed WHERE k = 1 ORDER BY k DESC NULLS LAST, ts LIMIT 3;
SELECT ts FROM test_lc_float_fixed WHERE k = 1 ORDER BY ts DESC LIMIT 3;
SELECT ts FROM test_nullable_float_fixed WHERE k = 1 ORDER BY k ASC NULLS FIRST, ts DESC LIMIT 3;
SELECT ts FROM test_nullable_float_fixed WHERE k = 1 ORDER BY ts DESC LIMIT 3;

SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT ts FROM test_lc_float_fixed WHERE k = 1 ORDER BY k DESC NULLS LAST, ts LIMIT 3 SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%Read type%';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT ts FROM test_lc_float_fixed WHERE k = 1 ORDER BY ts DESC LIMIT 3 SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%Read type%';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT ts FROM test_nullable_float_fixed WHERE k = 1 ORDER BY k ASC NULLS FIRST, ts DESC LIMIT 3 SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%Read type%';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT ts FROM test_nullable_float_fixed WHERE k = 1 ORDER BY ts DESC LIMIT 3 SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%Read type%';

-- The key is not fixed here, so the unsupported NULLS direction must still disable read-in-order.
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT ts FROM test_lc_float_fixed WHERE ts > 0 ORDER BY k DESC NULLS LAST, ts LIMIT 3 SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%Read type%';

DROP TABLE test_lc_float_fixed;
DROP TABLE test_nullable_float_fixed;

DROP TABLE test_lc_float_nan;
DROP TABLE test_lc_float32_nan;
DROP TABLE test_lc_bfloat16_nan;
DROP TABLE test_nullable_float_nan;
DROP TABLE test_nullable_int;
