-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- EXPLAIN output may differ

-- { echo }

SET use_query_condition_cache = 0;

-- divide(var, neg_const): is_always_monotonic=true, is_positive=false.
-- When the constant in the predicate is pushed through the chain, the comparison
-- operator must be flipped because the function is monotonically decreasing.
-- index_granularity = 1 makes each row its own granule, so `max_rows_to_read`
-- equals the number of granules the optimizer should select.

DROP TABLE IF EXISTS test_divide_neg;
CREATE TABLE test_divide_neg (c0 Int32) ENGINE = MergeTree() ORDER BY (c0 / -42) SETTINGS index_granularity = 1;
INSERT INTO test_divide_neg VALUES (-100), (-1), (0), (1), (100);

SELECT c0 FROM test_divide_neg WHERE c0 < 0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 4;
SELECT c0 FROM test_divide_neg WHERE c0 <= 0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 4;
SELECT c0 FROM test_divide_neg WHERE c0 > 0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT c0 FROM test_divide_neg WHERE c0 >= 0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT c0 FROM test_divide_neg WHERE c0 = -1 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT c0 FROM test_divide_neg WHERE c0 != 0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 5;

-- Reversed-argument form (constant on the left).
SELECT c0 FROM test_divide_neg WHERE 0 > c0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 4;
SELECT c0 FROM test_divide_neg WHERE 0 < c0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 3;

EXPLAIN indexes = 1
SELECT c0 FROM test_divide_neg WHERE c0 < 0;

-- intDiv(var, neg_const): integer result variant of the same case. The integer divisor
-- collapses multiple `c0` values to the same key (e.g. intDiv(-1,-42) = intDiv(0,-42) =
-- intDiv(1,-42) = 0), which makes the boundary granule wider and prunes fewer granules.

DROP TABLE IF EXISTS test_intdiv_neg;
CREATE TABLE test_intdiv_neg (c0 Int32) ENGINE = MergeTree() ORDER BY intDiv(c0, -42) SETTINGS index_granularity = 1;
INSERT INTO test_intdiv_neg VALUES (-100), (-1), (0), (1), (100);

SELECT c0 FROM test_intdiv_neg WHERE c0 < 0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 5;
SELECT c0 FROM test_intdiv_neg WHERE c0 <= 0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 5;
SELECT c0 FROM test_intdiv_neg WHERE c0 > 0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 4;
SELECT c0 FROM test_intdiv_neg WHERE c0 >= 0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 4;
SELECT c0 FROM test_intdiv_neg WHERE c0 = -1 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 4;
SELECT c0 FROM test_intdiv_neg WHERE c0 != 0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 5;

EXPLAIN indexes = 1
SELECT c0 FROM test_intdiv_neg WHERE c0 < 0;

-- Compound chain: two non-positive functions compose to a positive direction (no flip).

DROP TABLE IF EXISTS test_double_neg;
CREATE TABLE test_double_neg (c0 Int32) ENGINE = MergeTree() ORDER BY (intDiv(c0, -5) / -7) SETTINGS index_granularity = 1;
INSERT INTO test_double_neg VALUES (-100), (-1), (0), (1), (100);

SELECT c0 FROM test_double_neg WHERE c0 < 0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 4;
SELECT c0 FROM test_double_neg WHERE c0 > 0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 5;

EXPLAIN indexes = 1
SELECT c0 FROM test_double_neg WHERE c0 < 0;

-- Compound chain: one non-positive function, one positive (overall non-increasing).

DROP TABLE IF EXISTS test_mixed_neg;
CREATE TABLE test_mixed_neg (c0 Int32) ENGINE = MergeTree() ORDER BY (intDiv(c0, 5) / -7) SETTINGS index_granularity = 1;
INSERT INTO test_mixed_neg VALUES (-100), (-1), (0), (1), (100);

SELECT c0 FROM test_mixed_neg WHERE c0 < 0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 5;
SELECT c0 FROM test_mixed_neg WHERE c0 > 0 ORDER BY c0 SETTINGS force_primary_key = 1, max_rows_to_read = 4;

EXPLAIN indexes = 1
SELECT c0 FROM test_mixed_neg WHERE c0 < 0;

-- `negate` returns is_positive=false but is_always_monotonic=false, so the
-- constant-pushed-through-chain path is rejected and no pruning is attempted.

DROP TABLE IF EXISTS test_negate;
CREATE TABLE test_negate (c0 Int32) ENGINE = MergeTree() ORDER BY negate(c0) SETTINGS index_granularity = 1;
INSERT INTO test_negate VALUES (-100), (-1), (0), (1), (100);

SELECT c0 FROM test_negate WHERE c0 < 0 ORDER BY c0;
SELECT c0 FROM test_negate WHERE c0 > 0 ORDER BY c0;

EXPLAIN indexes = 1
SELECT c0 FROM test_negate WHERE c0 < 0;

-- `multiply(var, neg_const)` is is_always_monotonic=false (overflow risk), so it also
-- does not trigger the path.

DROP TABLE IF EXISTS test_multiply_neg;
CREATE TABLE test_multiply_neg (c0 Int32) ENGINE = MergeTree() ORDER BY (c0 * -2) SETTINGS index_granularity = 1;
INSERT INTO test_multiply_neg VALUES (-100), (-1), (0), (1), (100);

SELECT c0 FROM test_multiply_neg WHERE c0 < 0 ORDER BY c0;
SELECT c0 FROM test_multiply_neg WHERE c0 > 0 ORDER BY c0;

EXPLAIN indexes = 1
SELECT c0 FROM test_multiply_neg WHERE c0 < 0;

DROP TABLE test_divide_neg;
DROP TABLE test_intdiv_neg;
DROP TABLE test_double_neg;
DROP TABLE test_mixed_neg;
DROP TABLE test_negate;
DROP TABLE test_multiply_neg;
