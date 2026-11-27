SELECT a, sum(b) FROM (SELECT 1 AS a, 1 AS b, 0 AS c) GROUP BY a HAVING c SETTINGS enable_analyzer=1 -- { serverError NOT_AN_AGGREGATE }
