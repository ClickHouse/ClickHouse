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
