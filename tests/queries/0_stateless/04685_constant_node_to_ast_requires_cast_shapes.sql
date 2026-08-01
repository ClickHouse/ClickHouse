-- `ConstantNode::toAST` answers "does this constant need a `_CAST` wrapper?" from the result
-- type for `Nullable`/`Array`/`Tuple` instead of inferring the type of the materialized value.
-- Pin the emitted AST so that fast path stays equivalent to the inference it replaces.
-- `dump_tree = 0` keeps only the AST, which is the thing under test.
--
-- Not every row reaches that predicate, and the sections below say which do. Two things route
-- a constant past it: `ConstantNode.cpp:257` tests `source_expression` before calling
-- `requires_cast`, so a constant folded from a function short-circuits before the lambda; and
-- `FunctionNode.cpp:262-268` sends a non-`Array` `IN` set with no source expression down the
-- `add_cast_for_constants = false` path, which returns earlier still.

SET enable_analyzer = 1;

SELECT '-- Array: cast wrapper kept (this is the shape a large IN set takes)';
-- These six reach the predicate and are answered by the result type alone.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT rand() IN [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT rand() IN [-1, -2, -3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT rand() IN [1.5, 2.5];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT [1, 2, NULL];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT [[1], [2, 3]];
-- A non-numeric element type, to pin that the answer does not depend on the elements.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT ['a', 'b'];
-- The `LowCardinality` here is on the LEFT operand, so the constant is a plain `Array(UInt8)`.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toLowCardinality(rand()) IN [1, 2, 3];
-- These two fold with a source expression, so `ConstantNode.cpp:257` short-circuits.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST([1, 2], 'Array(Int64)');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST([1], 'Array(Nullable(UInt8))');

SELECT '-- Tuple: a bare IN set stays bare, a folded one and a bare tuple are wrapped';
-- Bare, so `FunctionNode.cpp:262-268` keeps it uncast and `toASTImpl` returns before the
-- predicate. `tuple(1, 2, 3)` folds with a source expression, which fails that same guard's
-- `hasSourceExpression` clause, so it is wrapped and also never reaches the predicate.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT (rand(), rand()) IN ((1, 2), (3, 4));
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT rand() IN tuple(1, 2, 3);
-- Reaches the predicate; the one below folds and short-circuits.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT (1, 'a', NULL);
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST((1, 2), 'Tuple(Int64, Int64)');

SELECT '-- Nullable: null and non-null both keep the wrapper';
-- The first row's Nullable is on the LEFT operand: the constant is `Array(UInt8)`, so it is an
-- Array witness. `NULL` is the only row here that reaches the predicate as a Nullable; the four
-- `CAST` rows fold with a source expression and short-circuit.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toNullable(rand()) IN [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(NULL, 'Nullable(UInt32)') IN [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(1, 'Nullable(UInt32)') IN [1, 2, 3];
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT NULL;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(1, 'Nullable(Int64)');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT CAST(1, 'Nullable(UInt8)');

SELECT '-- bare literals answered false: these witness an over-broad result-type test';
-- These four reach the predicate and must keep getting false, so widening it to any type
-- beyond Nullable/Array/Tuple shows up here as a spurious `_CAST`.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT 1;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT -1;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT 'str';
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT true;

SELECT '-- an IN set that is neither Array nor folded: returns before the predicate';
-- The constant is a `Tuple(String, String)`, which `FunctionNode.cpp:262-268` routes to
-- `add_cast_for_constants = false`. This guards the `isArray` clause added by #105894, not
-- the predicate.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT toString(rand()) IN ('a', 'b');

SELECT '-- folded constants: ConstantNode.cpp:257 short-circuits before the predicate';
-- Every row here carries a source expression, so its emitted AST is decided before the
-- predicate runs and is unchanged whatever the predicate answers. They are pinned so that a
-- future change to that short-circuit becomes visible.
--
-- That also means these rows cannot witness the predicate's type boundary. A live constant of
-- `LowCardinality`, `Map`, `Variant` or `Dynamic` is not reachable from SQL at all: every route
-- to such a type runs through a function and so sets a source expression (checked for `CAST`,
-- a scalar subquery and a query parameter), and bare parser literals are only scalars, `Array`
-- or `Tuple` (`QueryTreeBuilder.cpp:698`). So the boundary is not held by any row below; it is
-- held by the type-index equality in `requiresCastCallForResultType` alone. In particular,
-- adding `isLowCardinality` to that predicate would leave both `LowCardinality` rows here
-- byte-identical.
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
