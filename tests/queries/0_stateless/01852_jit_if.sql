SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS test_jit_nonnull;
CREATE TABLE test_jit_nonnull (value UInt8) ENGINE = TinyLog;
INSERT INTO test_jit_nonnull VALUES (0), (1);

SELECT 'test_jit_nonnull';
SELECT value, multiIf(value = 1, 2, value, 1, 0), if (value, 1, 0) FROM test_jit_nonnull;

DROP TABLE IF EXISTS test_jit_nullable;
CREATE TABLE test_jit_nullable (value Nullable(UInt8)) ENGINE = TinyLog;
INSERT INTO test_jit_nullable VALUES (0), (1), (NULL);

SELECT 'test_jit_nullable';
SELECT value, multiIf(value = 1, 2, value, 1, 0), if (value, 1, 0) FROM test_jit_nullable;

DROP TABLE test_jit_nonnull;
DROP TABLE test_jit_nullable;
