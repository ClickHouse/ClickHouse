SELECT 1 AS x, [2, 3] AS y, 'Hello' AS z, NULL AS a FORMAT ODBCDriver2;
SELECT number % 10 AS k, count() AS c FROM numbers(100) GROUP BY k WITH TOTALS FORMAT ODBCDriver2;
