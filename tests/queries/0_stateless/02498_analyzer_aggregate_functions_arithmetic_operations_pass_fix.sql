SET enable_analyzer = 1;
SET optimize_arithmetic_operations_in_aggregate_functions = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value UInt64
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table VALUES (1, 1);
INSERT INTO test_table VALUES (1, 1);

SELECT sum((2 * id) as func), func FROM test_table GROUP BY id;

SELECT max(100-number), min(100-number) FROM numbers(2);

select (sum(toDecimal64(2.11, 15) - number), 1) FROM numbers(2);
