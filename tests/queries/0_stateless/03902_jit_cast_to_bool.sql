-- Test for JIT compilation of CAST to Bool/Nullable(Bool)
-- Previously, nativeBoolCast returned i1 but Bool (UInt8) needs i8,
-- causing LLVM assertion failure when inserting into Nullable struct.

SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS test_jit_bool;
CREATE TABLE test_jit_bool (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO test_jit_bool SELECT number, number % 3 FROM numbers(100);

-- Test CAST to Bool with JIT
SELECT CAST(a, 'Bool') AS result FROM test_jit_bool WHERE a < 3 ORDER BY a;

-- Test CAST to Nullable(Bool) with JIT - this was crashing before the fix
SELECT CAST(a = b, 'Nullable(Bool)') AS result FROM test_jit_bool WHERE a < 5 ORDER BY a;

-- Test in complex expression that triggers JIT compilation
SELECT if(CAST(a = b, 'Nullable(Bool)'), sign(a), b) - a AS result
FROM test_jit_bool
WHERE a < 5
ORDER BY a;

DROP TABLE test_jit_bool;
