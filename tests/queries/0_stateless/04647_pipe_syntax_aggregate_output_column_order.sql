-- The output of pipe `AGGREGATE` is the grouping columns, in `GROUP BY` order, followed by the
-- remaining expressions. A grouping expression the user already projected after an aggregate must be
-- moved to the front rather than left in place, otherwise a following stage using positional
-- semantics (`|> ORDER BY 1`) would sort by the aggregate instead of by the first grouping column.
SET allow_experimental_pipe_syntax = 1;
SET enable_positional_arguments = 1;

SELECT 'aggregate before grouping key';
EXPLAIN SYNTAX FROM numbers(5) |> AGGREGATE count() AS c, number % 3 AS k GROUP BY k;
FROM numbers(5) |> AGGREGATE count() AS c, number % 3 AS k GROUP BY k |> ORDER BY 1;

SELECT 'grouping key before aggregate produces the same query';
EXPLAIN SYNTAX FROM numbers(5) |> AGGREGATE number % 3 AS k, count() AS c GROUP BY k;
FROM numbers(5) |> AGGREGATE number % 3 AS k, count() AS c GROUP BY k |> ORDER BY 1;

SELECT 'grouping key matched by its alias-less form';
EXPLAIN SYNTAX FROM numbers(5) |> AGGREGATE count() AS c, number % 3 AS k GROUP BY number % 3;
FROM numbers(5) |> AGGREGATE count() AS c, number % 3 AS k GROUP BY number % 3 |> ORDER BY 1;

SELECT 'several grouping keys are ordered as written in GROUP BY';
EXPLAIN SYNTAX FROM numbers(6) |> AGGREGATE count() AS c, number % 3 AS m, number % 2 AS k GROUP BY k, m;
FROM numbers(6) |> AGGREGATE count() AS c, number % 3 AS m, number % 2 AS k GROUP BY k, m |> ORDER BY 1, 2;

SELECT 'a grouping key that is not projected is still added in front';
EXPLAIN SYNTAX FROM numbers(6) |> AGGREGATE count() AS c GROUP BY number % 2;
FROM numbers(6) |> AGGREGATE count() AS c GROUP BY number % 2 |> ORDER BY 1;

-- A positional grouping argument resolves against the projection, so neither adding nor reordering
-- columns may happen: the projection is passed through exactly as written, like the regular SELECT.
SELECT 'positional grouping argument leaves the projection untouched';
EXPLAIN SYNTAX FROM numbers(6) |> AGGREGATE number % 2 AS k, count() AS c GROUP BY 1;
FROM numbers(6) |> AGGREGATE number % 2 AS k, count() AS c GROUP BY 1 |> ORDER BY 1;
EXPLAIN SYNTAX FROM numbers(6) |> AGGREGATE count() AS c, number % 2 AS k GROUP BY 2;
FROM numbers(6) |> AGGREGATE count() AS c, number % 2 AS k GROUP BY 2 |> ORDER BY 2;

-- Mixing a positional and a non-positional grouping argument must not shift the positions either.
SELECT 'mixed positional and expression grouping arguments';
EXPLAIN SYNTAX FROM numbers(6) |> AGGREGATE count() AS c, number % 2 AS k GROUP BY 2, number % 3;
FROM numbers(6) |> AGGREGATE count() AS c, number % 2 AS k GROUP BY 2, number % 3 |> ORDER BY 2, 1;

-- `GROUP BY ALL` does not spell out the grouping keys, so the projection keeps the written order.
SELECT 'GROUP BY ALL keeps the written order';
EXPLAIN SYNTAX FROM numbers(6) |> AGGREGATE count() AS c, number % 2 AS k GROUP BY ALL;
FROM numbers(6) |> AGGREGATE count() AS c, number % 2 AS k GROUP BY ALL |> ORDER BY 2;
