-- ConstantNode::toAST() answers "does this constant need a _CAST wrapper?" from the result type
-- for Nullable/Array/Tuple instead of inferring the type of the materialized value. Pin the
-- emitted AST for every wrapper shape, so that fast path stays equivalent to the inference it
-- replaces. `dump_tree = 0` keeps only the AST, which is the thing under test.

SET enable_analyzer = 1;

SELECT '-- Array: cast wrapper kept (this is the shape a large IN set takes)';
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT rand() IN [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT rand() IN [-1, -2, -3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT rand() IN [1.5, 2.5];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT [1, 2, NULL];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT [[1], [2, 3]];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST([1, 2], 'Array(Int64)');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST([1], 'Array(Nullable(UInt8))');

SELECT '-- Tuple: an IN set stays bare, a bare tuple constant is wrapped';
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT (rand(), rand()) IN ((1, 2), (3, 4));
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT rand() IN tuple(1, 2, 3);
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT (1, 'a', NULL);
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST((1, 2), 'Tuple(Int64, Int64)');

SELECT '-- Nullable: null and non-null both keep the wrapper';
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toNullable(rand()) IN [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(NULL, 'Nullable(UInt32)') IN [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(1, 'Nullable(UInt32)') IN [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT NULL;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(1, 'Nullable(Int64)');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(1, 'Nullable(UInt8)');

SELECT '-- shapes the fast path must NOT claim: they keep taking the value-inference path';
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toLowCardinality(rand()) IN [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toString(rand()) IN ('a', 'b');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST('a', 'LowCardinality(String)');
-- LowCardinality(Nullable(T)) reports isLowCardinality(), not isNullable(), so it is the shape
-- most likely to be mis-claimed by a result-type test.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(1, 'LowCardinality(Nullable(UInt8))')
SETTINGS allow_suspicious_low_cardinality_types = 1;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(map('a', 1), 'Map(String, Int64)');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(CAST(1, 'UInt64'), 'Variant(UInt64, String)');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST([1, 2], 'Dynamic');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT 1;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT -1;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(1, 'Int8');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT 'str';
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT true;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toDate('2020-01-01');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toDateTime64('2020-01-01 00:00:00.123', 3);
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toDecimal64(1.5, 2);

SELECT '-- the value that reaches a remote server is unchanged (why the wrapper exists)';
SELECT countIf(dummy IN [1, -1]) FROM remote('127.0.0.{1,2}', 'system', 'one')
SETTINGS empty_result_for_aggregation_by_empty_set = 0;
SELECT number IN [1, 2, 3] FROM remote('127.0.0.1', numbers(2)) ORDER BY number;
