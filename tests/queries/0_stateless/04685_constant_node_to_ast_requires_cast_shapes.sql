-- `ConstantNode::toAST` answers "does this constant need a `_CAST` wrapper?" from the result type
-- for `Nullable`/`Array`/`Tuple` instead of inferring the type of the materialized value. Pin the
-- emitted AST so that fast path stays equivalent to the inference it replaces; `dump_tree = 0` keeps only the AST.
-- Not every row reaches that predicate: `ConstantNode::toASTImpl` tests `source_expression` first, so a
-- folded constant short-circuits, and the `IN` branch that disables `add_cast_for_constants` returns earlier still.

SET enable_analyzer = 1;

SELECT '-- Array: cast wrapper kept (this is the shape a large IN set takes)';
-- The first eight reach the predicate; the last two fold and short-circuit. `toLowCardinality` is on
-- the left operand, so that row's constant is a plain `Array(UInt8)`.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT rand() IN [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT rand() IN [-1, -2, -3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT rand() IN [1.5, 2.5];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT [1, 2, NULL];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT [[1], [2, 3]];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT ['a', 'b'];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toLowCardinality(rand()) IN [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST([1, 2], 'Array(Int64)');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST([1], 'Array(Nullable(UInt8))');

SELECT '-- Tuple: a bare IN set stays bare, a folded one and a bare tuple are wrapped';
-- Only `(1, 'a', NULL)` reaches the predicate: the bare set returns before it, the two folded rows short-circuit.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT (rand(), rand()) IN ((1, 2), (3, 4));
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT rand() IN tuple(1, 2, 3);
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT (1, 'a', NULL);
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST((1, 2), 'Tuple(Int64, Int64)');

SELECT '-- Nullable: null and non-null both keep the wrapper';
-- The first row and `NULL` reach the predicate; the first row's `Nullable` is on the left operand, so
-- its constant is an `Array(UInt8)`. The four `CAST` rows fold and short-circuit.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toNullable(rand()) IN [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(NULL, 'Nullable(UInt32)') IN [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(1, 'Nullable(UInt32)') IN [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT NULL;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(1, 'Nullable(Int64)');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(1, 'Nullable(UInt8)');

SELECT '-- bare literals answered false: these witness an over-broad result-type test';
-- All four reach the predicate and must keep answering false, so widening it beyond `Nullable`/`Array`/`Tuple` shows up here as a spurious `_CAST`.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT 1;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT -1;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT 'str';
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT true;

SELECT '-- an IN set that is neither Array nor folded: returns before the predicate';
-- This row guards the `isArray` clause added by #105894 instead of the predicate.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toString(rand()) IN ('a', 'b');

SELECT '-- folded constants: the source-expression check short-circuits before the predicate';
-- No row can witness the predicate's boundary for `LowCardinality`, `Map`, `Variant` or `Dynamic`: such a constant
-- only arises through a function, which sets a source expression, so that boundary is held by `requiresCastCall`'s type-id equality alone.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST('a', 'LowCardinality(String)');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(1, 'LowCardinality(Nullable(UInt8))')
SETTINGS allow_suspicious_low_cardinality_types = 1;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(map('a', 1), 'Map(String, Int64)');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(CAST(1, 'UInt64'), 'Variant(UInt64, String)');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST([1, 2], 'Dynamic');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(1, 'Int8');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toDate('2020-01-01');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toDateTime64('2020-01-01 00:00:00.123', 3);
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toDecimal64(1.5, 2);

SELECT '-- the value that reaches a remote server is unchanged (why the wrapper exists)';
SELECT countIf(dummy IN [1, -1]) FROM remote('127.0.0.{1,2}', 'system', 'one')
SETTINGS empty_result_for_aggregation_by_empty_set = 0;
SELECT number IN [1, 2, 3] FROM remote('127.0.0.1', numbers(2)) ORDER BY number;
