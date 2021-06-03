SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (a UInt64) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO test_table VALUES (1);

SELECT test_table.a FROM test_table ORDER BY (test_table.a > test_table.a) + 1;
SELECT test_table.a FROM test_table ORDER BY (test_table.a >= test_table.a) + 1;

SELECT test_table.a FROM test_table ORDER BY (test_table.a < test_table.a) + 1;
SELECT test_table.a FROM test_table ORDER BY (test_table.a <= test_table.a) + 1;

SELECT test_table.a FROM test_table ORDER BY (test_table.a == test_table.a) + 1;
SELECT test_table.a FROM test_table ORDER BY (test_table.a != test_table.a) + 1;
