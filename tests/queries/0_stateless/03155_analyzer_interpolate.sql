-- https://github.com/ClickHouse/ClickHouse/issues/62464
SET allow_experimental_analyzer = 1;

SELECT n, [number] AS inter FROM (
   SELECT toFloat32(number % 10) AS n, number
   FROM numbers(10) WHERE number % 3 = 1
) GROUP BY n, inter ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5 INTERPOLATE (inter AS [5]);

SELECT n, number+5 AS inter FROM (  -- { serverError NOT_AN_AGGREGATE }
   SELECT toFloat32(number % 10) AS n, number, number*2 AS mn
   FROM numbers(10) WHERE number % 3 = 1
) GROUP BY n, inter ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5 INTERPOLATE (inter AS mn * 2);
