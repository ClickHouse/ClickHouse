-- Positional GROUP BY arguments in the pipe AGGREGATE operator must resolve against the
-- AGGREGATE projection, exactly like the equivalent regular SELECT, instead of being prepended
-- to the projection as a literal grouping column (see ParserPipeOperators.cpp).

SET allow_experimental_pipe_syntax = 1;
SET enable_positional_arguments = 1;

-- GROUP BY 1 refers to the first projection column (number % 2 AS k), not to a prepended literal 1.
FROM numbers(6) |> AGGREGATE number % 2 AS k, count() AS c GROUP BY 1 |> ORDER BY k;
SELECT number % 2 AS k, count() AS c FROM numbers(6) GROUP BY 1 ORDER BY k;

-- Negative positional arguments count from the end of the projection.
FROM numbers(6) |> AGGREGATE number % 3 AS a, number % 2 AS b, count() AS c GROUP BY -3, -2 |> ORDER BY a, b;
SELECT number % 3 AS a, number % 2 AS b, count() AS c FROM numbers(6) GROUP BY -3, -2 ORDER BY a, b;

-- An out-of-bounds positional argument is rejected, matching the regular SELECT.
FROM numbers(6) |> AGGREGATE number % 2 AS k, count() AS c GROUP BY 5; -- { serverError BAD_ARGUMENTS }
SELECT number % 2 AS k, count() AS c FROM numbers(6) GROUP BY 5; -- { serverError BAD_ARGUMENTS }

-- The pipe form produces exactly the same AST as the equivalent regular SELECT: the positional
-- GROUP BY 1 is preserved and no literal grouping column is prepended to the projection.
EXPLAIN SYNTAX FROM numbers(6) |> AGGREGATE number % 2 AS k, count() AS c GROUP BY 1;
EXPLAIN SYNTAX SELECT number % 2 AS k, count() AS c FROM numbers(6) GROUP BY 1;
