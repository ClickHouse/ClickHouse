DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

EXPLAIN SYNTAX run_query_tree_passes=1
WITH 1 AS compound_value SELECT * APPLY (x -> compound_value.*)
FROM test_table WHERE x > 0
SETTINGS convert_query_to_cnf = true, optimize_using_constraints = true, optimize_substitute_columns = true; -- { serverError UNKNOWN_IDENTIFIER,UNSUPPORTED_METHOD }

DROP TABLE test_table;
