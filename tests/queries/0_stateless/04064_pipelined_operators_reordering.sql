SET allow_experimental_pipe_syntax = 1;

-- FROM-first without any pipe operators
FROM numbers(3);

-- Multiple WHEREs merge into AND without wrapping
FROM numbers(10) |> WHERE number >= 3 |> WHERE number < 7 |> ORDER BY number;

-- WHERE after ORDER BY wraps into subquery
FROM numbers(5) |> ORDER BY number DESC |> WHERE number >= 2 |> ORDER BY number;

-- ORDER BY WITH FILL then WHERE: fill generates rows, then filter applies
FROM numbers(3) |> ORDER BY number WITH FILL FROM 0 TO 5 |> WHERE number < 4;

-- LIMIT + LIMIT: second wraps the first
FROM numbers(10) |> ORDER BY number |> LIMIT 7 |> LIMIT 3;

-- ORDER BY + ORDER BY: second wraps the first
FROM numbers(5) |> ORDER BY number DESC |> ORDER BY number ASC;

-- Full-table AGGREGATE without GROUP BY
FROM numbers(5) |> AGGREGATE count() AS total;

-- Double AGGREGATE: each stage wraps
FROM numbers(6) |> AGGREGATE number % 2 AS k, count() AS c GROUP BY number % 2 |> AGGREGATE sum(c) AS total;

-- AGGREGATE + WHERE + ORDER BY chain
FROM numbers(10) |> AGGREGATE number % 3 AS k, count() AS c GROUP BY number % 3 |> WHERE c >= 4 |> ORDER BY k;

-- WHERE + ORDER BY + WHERE: second WHERE wraps to preserve ORDER BY semantics
FROM numbers(10) |> WHERE number < 8 |> ORDER BY number DESC |> WHERE number > 2 |> ORDER BY number;

-- ORDER BY goes after WHERE following pipe order
FROM numbers(3) |> ORDER BY number WITH FILL FROM 0 TO 5 |> WHERE number < 4;

-- Test for ALIAS_REQUIRED setting
FROM numbers(3) |> ORDER BY number |> ORDER BY number |> JOIN numbers(1) AS r ON 1;

-- Pipeline order is preserved for JOIN
FROM numbers(2) |> AGGREGATE count() AS c |> JOIN numbers(2) AS r ON 1;

-- Pipe order is preserved for WHERE and AGGREGATE
FROM numbers(3) |> AGGREGATE count() AS c |> WHERE c = 3;

-- Multiple AGGREGATE and WHERE test
FROM numbers(10) |> AGGREGATE number % 3 AS k, count() AS c GROUP BY k |> WHERE c > 3 |> AGGREGATE sum(c) AS s;

-- AGGREGATE -> ORDER BY -> WHERE must preserve pipe order
FROM numbers(5) |> AGGREGATE count() AS c |> ORDER BY c |> WHERE c = 5;

-- Multiple joins test
FROM numbers(2) AS l |> JOIN numbers(2) AS r ON 1 |> JOIN numbers(2) AS z ON 1 |> AGGREGATE count() AS c;

FROM numbers(10) |> ORDER BY number DESC |> LIMIT 3 |> JOIN numbers(1) AS r ON 1 |> WHERE number > 7 |> ORDER BY number;