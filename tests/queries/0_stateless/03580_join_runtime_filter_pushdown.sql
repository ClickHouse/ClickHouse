SET enable_analyzer=1;

SELECT REGEXP_REPLACE(explain, '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID')
FROM (

EXPLAIN
SELECT *
FROM (
    SELECT a.number AS a_number, b.number AS b_number
    FROM numbers(10) AS a
        JOIN numbers(10) AS b
        ON a.number%2 = b.number%3
    ) AS ab
    JOIN numbers(10) AS c
    ON b_number = c.number+2
SETTINGS enable_join_runtime_filters=1

);
