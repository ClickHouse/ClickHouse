-- Tests JIT compilation of aggregate functions on Decimal types.
-- PR #88770 added JIT for these paths but existing tests only covered integer/float types.
-- The key correctness concern is signed comparison: Decimal wrappers need special handling
-- because std::numeric_limits<Decimal<T>>::is_signed is false.

SET compile_aggregate_expressions = 1;
SET min_count_to_compile_aggregate_expression = 0;

SELECT 'Test min/max with Decimal32';

DROP TABLE IF EXISTS test_jit_decimal32;
CREATE TABLE test_jit_decimal32 (id UInt64, val Decimal32(4)) ENGINE = TinyLog;
INSERT INTO test_jit_decimal32 VALUES (0, 5.1234), (0, -10.5678), (0, 3.0), (0, -1.0), (0, 7.9999);
INSERT INTO test_jit_decimal32 VALUES (1, -100.0), (1, -200.5), (1, -0.0001);
SELECT id, min(val), max(val) FROM test_jit_decimal32 GROUP BY id ORDER BY id;
DROP TABLE test_jit_decimal32;

SELECT 'Test min/max with Decimal64';

DROP TABLE IF EXISTS test_jit_decimal64;
CREATE TABLE test_jit_decimal64 (id UInt64, val Decimal64(8)) ENGINE = TinyLog;
INSERT INTO test_jit_decimal64 VALUES (0, 5.12345678), (0, -10.5), (0, 3.0), (0, -1.0), (0, 7.99999999);
INSERT INTO test_jit_decimal64 VALUES (1, -100.0), (1, -200.5), (1, -0.00000001);
SELECT id, min(val), max(val) FROM test_jit_decimal64 GROUP BY id ORDER BY id;
DROP TABLE test_jit_decimal64;

SELECT 'Test min/max with Decimal128';

DROP TABLE IF EXISTS test_jit_decimal128;
CREATE TABLE test_jit_decimal128 (id UInt64, val Decimal128(18)) ENGINE = TinyLog;
INSERT INTO test_jit_decimal128 VALUES (0, 5.0), (0, -10.0), (0, 3.0), (0, -1.0), (0, 7.0);
INSERT INTO test_jit_decimal128 VALUES (1, -100.0), (1, -200.5), (1, -0.000000000000000001);
SELECT id, min(val), max(val) FROM test_jit_decimal128 GROUP BY id ORDER BY id;
DROP TABLE test_jit_decimal128;

SELECT 'Test sum with Decimal types';

DROP TABLE IF EXISTS test_jit_decimal_sum;
CREATE TABLE test_jit_decimal_sum (id UInt64, d32 Decimal32(2), d64 Decimal64(4)) ENGINE = TinyLog;
INSERT INTO test_jit_decimal_sum VALUES (0, 10.50, 100.1234), (0, -3.25, -50.5678), (0, 7.75, 200.0);
INSERT INTO test_jit_decimal_sum VALUES (1, -1.00, -1.0000), (1, -2.00, -2.0000);
SELECT id, sum(d32), sum(d64) FROM test_jit_decimal_sum GROUP BY id ORDER BY id;
DROP TABLE test_jit_decimal_sum;

SELECT 'Test avg with Decimal types';

DROP TABLE IF EXISTS test_jit_decimal_avg;
CREATE TABLE test_jit_decimal_avg (id UInt64, val Decimal64(4)) ENGINE = TinyLog;
INSERT INTO test_jit_decimal_avg VALUES (0, 10.0), (0, 20.0), (0, 30.0);
INSERT INTO test_jit_decimal_avg VALUES (1, -5.0), (1, 15.0);
SELECT id, avg(val) FROM test_jit_decimal_avg GROUP BY id ORDER BY id;
DROP TABLE test_jit_decimal_avg;

SELECT 'Test Nullable(Decimal) min/max';

DROP TABLE IF EXISTS test_jit_nullable_decimal;
CREATE TABLE test_jit_nullable_decimal (id UInt64, val Nullable(Decimal64(4))) ENGINE = TinyLog;
INSERT INTO test_jit_nullable_decimal VALUES (0, 5.0), (0, -10.0), (0, NULL), (0, 7.0);
INSERT INTO test_jit_nullable_decimal VALUES (1, NULL), (1, NULL);
INSERT INTO test_jit_nullable_decimal VALUES (2, -3.0), (2, NULL), (2, -1.0);
SELECT id, min(val), max(val) FROM test_jit_nullable_decimal GROUP BY id ORDER BY id;
DROP TABLE test_jit_nullable_decimal;

SELECT 'Test Nullable(Decimal) sum/avg';

DROP TABLE IF EXISTS test_jit_nullable_decimal_sum;
CREATE TABLE test_jit_nullable_decimal_sum (id UInt64, val Nullable(Decimal64(2))) ENGINE = TinyLog;
INSERT INTO test_jit_nullable_decimal_sum VALUES (0, 10.50), (0, NULL), (0, -3.25);
INSERT INTO test_jit_nullable_decimal_sum VALUES (1, NULL), (1, NULL);
SELECT id, sum(val), avg(val) FROM test_jit_nullable_decimal_sum GROUP BY id ORDER BY id;
DROP TABLE test_jit_nullable_decimal_sum;
