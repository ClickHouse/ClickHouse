-- Tags: no-parallel-replicas

-- The rewrite of `x = c1 OR ... OR x = cN` into `x IN (c1, ..., cN)` appended the IN after all other OR operands.
-- OR is evaluated lazily from left to right, so the guard `number = 0` stopped protecting `intDiv(1, number)`
-- once three equalities were present. The IN must take the position of the first equality of the chain.
-- Found by json_ast_sql_execution_fuzzer (differential oracle, optimize_min_equality_disjunction_chain_length = 1).

SET enable_analyzer = 1;

SELECT (number = 0) OR (intDiv(1, number) != 0) OR (number = 2) OR (number = 3) FROM numbers(4);
SELECT (number = 0) OR (number = 2) OR (intDiv(1, number) != 0) OR (number = 3) FROM numbers(4);
SELECT (number = 0) OR (intDiv(1, number) != 0) OR (number = 2) OR (intDiv(1, number - 2) != 0) FROM numbers(4) SETTINGS optimize_min_equality_disjunction_chain_length = 1;

-- The IN replaces the first equality; the other operands keep their order.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT (number = 0) OR (intDiv(1, number) != 0) OR (number = 2) OR (number = 3) FROM numbers(4);
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT (intDiv(1, number) != 0) OR (number = 0) OR (number = 2) OR (toString(number) = '5') OR (number = 3) OR (toString(number) = '6') OR (toString(number) = '7') FROM numbers(4);

-- A guard written after the division does not protect it, with or without the rewrite.
SELECT (intDiv(1, number) != 0) OR (number = 0) OR (number = 2) OR (number = 3) FROM numbers(4); -- { serverError ILLEGAL_DIVISION }

-- A later equality behind a throwing operand: the short-circuit `or` evaluates its cheap arguments (the equalities)
-- eagerly and a lazily evaluated argument only on the rows where they are all false, so `number = 2` protects
-- `intDiv(1, number - 2)` whether it stands after it (no rewrite) or is merged into the leading `IN`.
SELECT (number = 0) OR (intDiv(1, number - 2) != 0) OR (number = 2) OR (number = 3) FROM numbers(4);
SELECT (number = 0) OR (intDiv(1, number - 2) != 0) OR (number = 2) OR (number = 3) FROM numbers(4) SETTINGS optimize_min_equality_disjunction_chain_length = 100;
SELECT (number = 0) OR (intDiv(1, number - 2) != 0) OR (number = 5) FROM numbers(4); -- { serverError ILLEGAL_DIVISION }
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT (number = 0) OR (intDiv(1, number - 2) != 0) OR (number = 2) OR (number = 3) FROM numbers(4);

-- Declined-chain rebuild (second repro from issue #121024). One chain in the OR is below
-- `optimize_min_equality_disjunction_chain_length` (declined, left as individual equalities) while another
-- chain in the SAME OR is merged into an IN. The declined chain must keep its equalities in their original
-- positions and the merged chain's IN must take the slot of its first equality - so a guard the user
-- wrote still runs before any throwing operand.
SELECT (number = 0) OR (intDiv(1, number) != 0) OR (toString(number) = 'x') OR (toString(number) = 'y') OR (number = 2) OR (number = 3) FROM numbers(4) SETTINGS optimize_min_equality_disjunction_chain_length = 3;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT (number = 0) OR (intDiv(1, number) != 0) OR (toString(number) = 'x') OR (toString(number) = 'y') OR (number = 2) OR (number = 3) FROM numbers(4) SETTINGS optimize_min_equality_disjunction_chain_length = 3;

-- Legacy AST path (`enable_analyzer = 0`). `TreeRewriter::analyzeSelect` runs `LogicalExpressionsOptimizer`
-- BEFORE the `ignore_ast_optimizations` gate, so this rewrite is reachable from projection materialization
-- via `ProjectionDescription::calculateByQuery` even though projections pass `.ignoreASTOptimizations()`.
-- The AST-side `addInExpression` must mirror the analyzer fix and put the IN at the first-equality slot.
SELECT (number = 0) OR (intDiv(1, number) != 0) OR (number = 2) OR (number = 3) FROM numbers(4) SETTINGS enable_analyzer = 0;
SELECT (number = 0) OR (number = 2) OR (intDiv(1, number) != 0) OR (number = 3) FROM numbers(4) SETTINGS enable_analyzer = 0;
SELECT (number = 0) OR (intDiv(1, number) != 0) OR (toString(number) = 'x') OR (toString(number) = 'y') OR (number = 2) OR (number = 3) FROM numbers(4) SETTINGS enable_analyzer = 0, optimize_min_equality_disjunction_chain_length = 3;

-- MergeTree evaluates a projection's SELECT during INSERT by building an `InterpreterSelectQuery` with
-- `.ignoreASTOptimizations()`. That gate is checked AFTER `LogicalExpressionsOptimizer`, so the AST OR->IN
-- rewrite still runs here. Without the slot-preserving fix, the `k = 0` guard would stop protecting
-- `intDiv(1, k)` and the INSERT (row k = 0) would fail with ILLEGAL_DIVISION.
DROP TABLE IF EXISTS t_05237_projection;
CREATE TABLE t_05237_projection
(
    k UInt64,
    v UInt64,
    PROJECTION p (SELECT k, v WHERE (k = 0) OR (intDiv(1, k) != 0) OR (k = 2) OR (k = 3))
) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05237_projection SELECT number, number + 100 FROM numbers(4) SETTINGS enable_analyzer = 0, optimize_min_equality_disjunction_chain_length = 2;
SELECT k, v FROM t_05237_projection ORDER BY k;
DROP TABLE t_05237_projection;
