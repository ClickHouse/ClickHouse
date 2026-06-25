-- Tags: no-random-settings
-- ^ pins compile_expressions / group_by_use_nulls / allow_experimental_correlated_subqueries below.

-- A correlated scalar subquery whose body has an arithmetic expression over the outer GROUP BY key,
-- evaluated under group_by_use_nulls + WITH CUBE/ROLLUP, builds an internally inconsistent ActionsDAG:
-- decorrelation feeds a Nullable(UInt64) value into `minus` nodes that were resolved (and keep their
-- result type) for the non-Nullable UInt64 key. The interpreter tolerates this; expression JIT used to
-- bake the declared types and abort in llvm::BinaryOperator::Create ("operands of differing type").
-- The JIT now skips such nodes and falls back to the interpreter, which produces the correct result.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET group_by_use_nulls = 1;
SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

SELECT number, (SELECT 1 - (number - 2)) AS x FROM numbers(2) GROUP BY number WITH CUBE ORDER BY number NULLS LAST;
SELECT number, (SELECT 65537 - (number - 2147483647)) AS x FROM numbers(2) GROUP BY number WITH ROLLUP ORDER BY number NULLS LAST;
SELECT number, (SELECT (number - 1) - (number - 2) + 3) AS x FROM numbers(2) GROUP BY number WITH CUBE ORDER BY number NULLS LAST;

-- Original AST-fuzzer reproducer (STID 1176-28cc): just check it does not crash the server.
SELECT DISTINCT ignore((SELECT maxMapStateOrDefault([number, NULL], [65537 - (number - 2147483647), number % -2147483648]) IGNORE NULLS))
FROM numbers(1) GROUP BY '', number WITH CUBE;
