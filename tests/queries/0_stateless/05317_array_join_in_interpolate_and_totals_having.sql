-- a fill row is built from one previous row, so arrayJoin cannot expand it
SELECT n, v FROM (SELECT number * 2 AS n, number AS v FROM numbers(3)) ORDER BY n WITH FILL INTERPOLATE (v AS arrayJoin([v, v])); -- { serverError UNSUPPORTED_METHOD }
SELECT n, v FROM (SELECT number * 2 AS n, number AS v FROM numbers(3)) ORDER BY n WITH FILL INTERPOLATE (v AS unnest([v])); -- { serverError UNSUPPORTED_METHOD }

-- an interpolated column that is itself an arrayJoin of the projection is fine
SELECT n, arrayJoin([v * 10]) AS w FROM (SELECT number * 2 AS n, number AS v FROM numbers(2)) ORDER BY n WITH FILL INTERPOLATE (w AS w + 1);

-- HAVING with TOTALS is rejected before execution, also through an alias and in EXPLAIN
SELECT number % 2 AS k, count() AS c FROM numbers(10) GROUP BY k WITH TOTALS HAVING arrayJoin([c]) > 0; -- { serverError ILLEGAL_COLUMN }
SELECT number % 2 AS k, count() AS c, arrayJoin([c]) AS y FROM numbers(10) GROUP BY k WITH TOTALS HAVING y > 0; -- { serverError ILLEGAL_COLUMN }
EXPLAIN SELECT number % 2 AS k, count() AS c FROM numbers(10) GROUP BY k WITH TOTALS HAVING arrayJoin([c]) > 0; -- { serverError ILLEGAL_COLUMN }

-- without TOTALS, or with arrayJoin only in the projection, it works
SELECT number % 2 AS k, count() AS c FROM numbers(10) GROUP BY k HAVING arrayJoin([c, c]) > 0 ORDER BY k;
SELECT number % 2 AS k, arrayJoin([count(), 1]) AS y FROM numbers(10) GROUP BY k WITH TOTALS HAVING count() > 0 ORDER BY k, y;

-- an arrayJoin computed before HAVING, as a GROUP BY key or inside an aggregate, is fine
SELECT arrayJoin(tags) AS tag, count() AS c FROM (SELECT ['a', 'b', ''] AS tags FROM numbers(4)) GROUP BY tag WITH TOTALS HAVING tag != '' ORDER BY tag;
SELECT number % 3 AS k, max(arrayJoin([number, number * 10])) AS m FROM numbers(10) GROUP BY k WITH TOTALS HAVING max(arrayJoin([number, number * 10])) > 75 ORDER BY k;
SELECT arrayJoin(tags) AS tag, count() AS c FROM (SELECT ['a', 'b', ''] AS tags FROM numbers(4)) GROUP BY tag WITH TOTALS HAVING arrayJoin([tag]) != ''; -- { serverError ILLEGAL_COLUMN }

-- with group_by_use_nulls, HAVING reads the key in its Nullable form
SELECT arrayJoin([0, 2, 4]) AS n, count() FROM numbers(3) GROUP BY GROUPING SETS ((n)) WITH TOTALS HAVING n > 0 ORDER BY n SETTINGS group_by_use_nulls = 1;
SELECT number AS n, count() FROM numbers(3) GROUP BY GROUPING SETS ((n)) WITH TOTALS HAVING arrayJoin([n, n + 1]) > 0 SETTINGS group_by_use_nulls = 1; -- { serverError ILLEGAL_COLUMN }

-- INTERPOLATE can read an arrayJoin of the projection or of a GROUP BY key
SELECT n, v, arrayJoin([7, 8]) AS a FROM (SELECT number * 2 AS n, number * 100 AS v FROM numbers(3)) ORDER BY n WITH FILL, a INTERPOLATE (v AS v + a);
SELECT arrayJoin([0, 2, 4]) AS n, count() AS c FROM numbers(3) GROUP BY n ORDER BY n WITH FILL INTERPOLATE (c AS c + n);
SELECT n, v, arrayJoin([7, 8]) AS a FROM (SELECT number * 2 AS n, number * 100 AS v FROM numbers(3)) ORDER BY n WITH FILL, a INTERPOLATE (v AS v + arrayJoin([1, 2])); -- { serverError UNSUPPORTED_METHOD }

-- the unnest alias is caught with function name normalization off too
SET normalize_function_names = 0;
SELECT n, v FROM (SELECT number * 2 AS n, number AS v FROM numbers(3)) ORDER BY n WITH FILL INTERPOLATE (v AS unnest([v])); -- { serverError UNSUPPORTED_METHOD }
SELECT number % 2 AS k, count() AS c FROM numbers(10) GROUP BY k WITH TOTALS HAVING unnest([c]) > 0; -- { serverError ILLEGAL_COLUMN }

