-- { echoOn }
SELECT argMax((n, n), n) t, toTypeName(t) FROM (SELECT if(number >= 100, NULL, number) AS n from numbers(10));
SELECT argMax((n, n), n) t, toTypeName(t) FROM (SELECT if(number <= 100, NULL, number) AS n from numbers(10));
SELECT argMax((n, n), n) t, toTypeName(t) FROM (SELECT if(number % 3 = 0, NULL, number) AS n from numbers(10));

SELECT argMax((n, n), n) t, toTypeName(t) FROM (SELECT if(number >= 100, NULL, number::Int32) AS n from numbers(10));
SELECT argMax((n, n), n) t, toTypeName(t) FROM (SELECT if(number <= 100, NULL, number::Int32) AS n from numbers(10));
SELECT argMax((n, n), n) t, toTypeName(t) FROM (SELECT if(number % 3 = 0, NULL, number::Int32) AS n from numbers(10));

SELECT argMin((n, n), n) t, toTypeName(t) FROM (SELECT if(number >= 100, NULL, number) AS n from numbers(5, 10));
SELECT argMin((n, n), n) t, toTypeName(t) FROM (SELECT if(number <= 100, NULL, number) AS n from numbers(5, 10));
SELECT argMin((n, n), n) t, toTypeName(t) FROM (SELECT if(number % 5 == 0, NULL, number) as n from numbers(5, 10));

SELECT argMin((n, n), n) t, toTypeName(t) FROM (SELECT if(number >= 100, NULL, number::Int32) AS n from numbers(5, 10));
SELECT argMin((n, n), n) t, toTypeName(t) FROM (SELECT if(number <= 100, NULL, number::Int32) AS n from numbers(5, 10));
SELECT argMin((n, n), n) t, toTypeName(t) FROM (SELECT if(number % 5 == 0, NULL, number::Int32) as n from numbers(5, 10));

SELECT argMaxIf((n, n), n, n > 100) t, toTypeName(t) FROM (SELECT if(number % 3 = 0, NULL, number) AS n from numbers(50));
SELECT argMaxIf((n, n), n, n < 100) t, toTypeName(t) FROM (SELECT if(number % 3 = 0, NULL, number) AS n from numbers(50));
SELECT argMaxIf((n, n), n, n % 5 == 0) t, toTypeName(t) FROM (SELECT if(number % 3 = 0, NULL, number) AS n from numbers(50));
