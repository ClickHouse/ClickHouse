-- https://github.com/ClickHouse/ClickHouse/issues/62464
SET allow_experimental_analyzer = 1;

SELECT n, [number] as inter FROM (
   SELECT toFloat32(number % 10) AS n, number
   FROM numbers(10) WHERE number % 3 = 1
) group by n, inter ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5 INTERPOLATE (inter AS [5]);
