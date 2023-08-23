FROM numbers(1) SELECT number;
WITH 1 as n FROM numbers(1) SELECT number * n;
FROM (FROM numbers(1) SELECT *) SELECT number;
FROM (FROM numbers(1) SELECT *) AS select SELECT number;
