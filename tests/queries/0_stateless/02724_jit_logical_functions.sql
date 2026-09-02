SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (a UInt8, b UInt8) ENGINE = TinyLog;
INSERT INTO test_table VALUES (0, 0), (0, 1), (1, 0), (1, 1);

SELECT 'Logical functions not null';
SELECT a, b, and(a, b), or(a, b), xor(a, b) FROM test_table;

DROP TABLE test_table;

DROP TABLE IF EXISTS test_table_nullable;
CREATE TABLE test_table_nullable (a UInt8, b Nullable(UInt8)) ENGINE = TinyLog;
INSERT INTO test_table_nullable VALUES (0, 0), (0, 1), (1, 0), (1, 1), (0, NULL), (1, NULL);

SELECT 'Logical functions nullable';
SELECT a, b, and(a, b), or(a, b), xor(a, b) FROM test_table_nullable;
SELECT and(b, b), or(b, b), xor(b, b) FROM test_table_nullable;

DROP TABLE test_table_nullable;
