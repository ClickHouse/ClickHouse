SELECT round(avgWeighted(x, y)) FROM (SELECT 1023 AS x, 1000000000 AS y UNION ALL SELECT 10 AS x, -9223372036854775808 AS y);
select avgWeighted(number, toDecimal128(number, 9)) from numbers(0);
SELECT avgWeighted(a, toDecimal64(c, 9)) OVER (PARTITION BY c) FROM (SELECT number AS a, number AS c FROM numbers(10));
select avg(toDecimal128(number, 9)) from numbers(0);
select avgWeighted(number, toDecimal128(0, 9)) from numbers(10);
