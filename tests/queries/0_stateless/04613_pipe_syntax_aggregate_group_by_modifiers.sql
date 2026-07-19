-- Grouping modifiers inside pipe AGGREGATE must behave exactly like the regular SELECT forms,
-- so each pipe query below is followed by its regular equivalent and both must print the same rows.
SET allow_experimental_pipe_syntax = 1;
SET group_by_use_nulls = 0;

SELECT 'GROUP BY ROLLUP';
FROM numbers(6) |> AGGREGATE number % 2 AS k, count() AS c GROUP BY ROLLUP(k) |> ORDER BY k, c;
SELECT number % 2 AS k, count() AS c FROM numbers(6) GROUP BY ROLLUP(k) ORDER BY k, c;

SELECT 'GROUP BY CUBE';
FROM numbers(6) |> AGGREGATE number % 2 AS k, number % 3 AS m, count() AS c GROUP BY CUBE(k, m) |> ORDER BY k, m, c;
SELECT number % 2 AS k, number % 3 AS m, count() AS c FROM numbers(6) GROUP BY CUBE(k, m) ORDER BY k, m, c;

SELECT 'GROUP BY GROUPING SETS';
FROM numbers(6) |> AGGREGATE number % 2 AS k, count() AS c GROUP BY GROUPING SETS((k), ()) |> ORDER BY k, c;
SELECT number % 2 AS k, count() AS c FROM numbers(6) GROUP BY GROUPING SETS((k), ()) ORDER BY k, c;

SELECT 'GROUP BY ALL';
FROM numbers(6) |> AGGREGATE number % 2 AS k, count() AS c GROUP BY ALL |> ORDER BY k, c;
SELECT number % 2 AS k, count() AS c FROM numbers(6) GROUP BY ALL ORDER BY k, c;

SELECT 'WITH ROLLUP';
FROM numbers(6) |> AGGREGATE number % 2 AS k, count() AS c GROUP BY k WITH ROLLUP |> ORDER BY k, c;
SELECT number % 2 AS k, count() AS c FROM numbers(6) GROUP BY k WITH ROLLUP ORDER BY k, c;

SELECT 'WITH CUBE';
FROM numbers(6) |> AGGREGATE number % 2 AS k, count() AS c GROUP BY k WITH CUBE |> ORDER BY k, c;
SELECT number % 2 AS k, count() AS c FROM numbers(6) GROUP BY k WITH CUBE ORDER BY k, c;

SELECT 'WITH TOTALS';
FROM numbers(6) |> AGGREGATE number % 2 AS k, count() AS c GROUP BY k WITH TOTALS |> ORDER BY k, c;
SELECT number % 2 AS k, count() AS c FROM numbers(6) GROUP BY k WITH TOTALS ORDER BY k, c;
