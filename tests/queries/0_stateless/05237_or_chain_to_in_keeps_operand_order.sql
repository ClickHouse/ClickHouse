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
