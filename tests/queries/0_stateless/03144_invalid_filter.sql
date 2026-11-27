-- https://github.com/ClickHouse/ClickHouse/issues/48049
SET enable_analyzer = 1;

CREATE TABLE test_table (`id` UInt64, `value` String) ENGINE = TinyLog() AS Select number, number::String from numbers(10);

WITH CAST(tuple(1), 'Tuple (value UInt64)') AS compound_value
SELECT id, test_table.* APPLY x -> compound_value.*
FROM test_table
WHERE arrayMap(x -> toString(x) AS lambda, [NULL, 256, 257, NULL, NULL])
SETTINGS convert_query_to_cnf = true, optimize_using_constraints = true, optimize_substitute_columns = true; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

DESCRIBE TABLE (SELECT test_table.COLUMNS(id) FROM test_table WHERE '2147483647'); -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

DROP TABLE test_table;
