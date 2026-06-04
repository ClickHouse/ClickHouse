SET allow_experimental_pipe_syntax = 1;

-- The pipelined parser must not shadow existing `FROM ... SELECT ...` queries.
FROM numbers(3) SELECT number;
FROM numbers(3) AS a SELECT a.number ORDER BY number;
FROM numbers(2) AS l JOIN numbers(2) AS r ON l.number = r.number SELECT l.number ORDER BY l.number;

-- EXPLAIN over the same forms must reach the regular SELECT parser too.
EXPLAIN SYNTAX FROM numbers(3) SELECT number;

-- Bare `FROM` (pipe form) and the pipe chain still work.
FROM numbers(3);
FROM numbers(3) |> WHERE number > 0;

-- AGGREGATE must not duplicate a grouping expression already present in the projection.
FROM numbers(6) |> AGGREGATE number % 2 AS k, count() AS c GROUP BY number % 2 |> ORDER BY k;
-- ... including when GROUP BY references the projection alias.
FROM numbers(6) |> AGGREGATE number % 2 AS k, count() AS c GROUP BY k |> ORDER BY k;
-- A grouping expression not present in the projection is still prepended.
FROM numbers(6) |> AGGREGATE count() AS c GROUP BY number % 2 |> ORDER BY 1;
