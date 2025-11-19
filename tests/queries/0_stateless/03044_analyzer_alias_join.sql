-- https://github.com/ClickHouse/ClickHouse/issues/17319
SET enable_analyzer=1;
CREATE TABLE hits (date Date, data Float64) engine=Memory();

SELECT
    subquery1.period AS period,
    if(1=1, 0, subquery1.data1) AS data,
    if(1=1, 0, subquery2.data) AS other_data
FROM
(
    SELECT date AS period, data AS data1
    FROM hits
) AS subquery1
LEFT JOIN
(
    SELECT date AS period, data AS data
    FROM hits
) AS subquery2 ON (subquery1.period = subquery2.period)
