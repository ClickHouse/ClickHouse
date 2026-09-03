SELECT sumKahan(toFloat64(number)) FROM numbers(10);
SELECT sumKahan(toNullable(toFloat64(number))) FROM numbers(10);
SELECT sum(toNullable(number)) FROM numbers(10);
SELECT sum(x) FROM (SELECT 1 AS x UNION ALL SELECT NULL);
SELECT sum(number) FROM numbers(10);
SELECT sum(number < 1000 ? NULL : number) FROM numbers(10);
