-- { echoOn }
SELECT groupArray(tuple(value)) OVER ()
FROM (select number value from numbers(10))
ORDER BY value ASC;

SELECT count() OVER (ORDER BY number + 1) FROM numbers(10) ORDER BY number;
