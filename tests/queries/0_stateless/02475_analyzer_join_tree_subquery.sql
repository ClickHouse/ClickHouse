SET enable_analyzer = 1;

WITH subquery AS (SELECT sum(number) FROM numbers(10)) SELECT * FROM subquery;

SELECT '--';

WITH subquery AS (SELECT sum(number) FROM numbers(10)) SELECT (SELECT * FROM subquery);
