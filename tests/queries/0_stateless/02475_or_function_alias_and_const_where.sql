SELECT (number = 1) AND (number = 2) AS value, sum(value) OVER () FROM numbers(1) WHERE 1;
SELECT (number = 1) AND (number = 2) AS value, sum(value) OVER () FROM numbers(1) WHERE 1 SETTINGS enable_analyzer=1;
