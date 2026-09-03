-- { echoOn }
SELECT groupArray(tuple(value)) OVER ()
FROM (select number value from numbers(10))
ORDER BY value ASC;

SELECT count() OVER (ORDER BY number + 1) FROM numbers(10) ORDER BY number;

SELECT count() OVER (ORDER BY number + 1) + 1 as foo FROM numbers(10)
ORDER BY foo;
