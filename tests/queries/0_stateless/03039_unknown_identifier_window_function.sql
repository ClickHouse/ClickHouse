-- https://github.com/ClickHouse/ClickHouse/issues/45535
SET enable_analyzer=1;

SELECT
  *,
  count() OVER () AS c
FROM numbers(10)
ORDER BY toString(number);


WITH
  toString(number) as str
SELECT
  *,
  count() OVER () AS c
FROM numbers(10)
ORDER BY str;

SELECT
  *,
  count() OVER () AS c,
  toString(number) as str
FROM numbers(10)
ORDER BY str;


WITH
  test AS (
    SELECT
      *,
      count() OVER () AS c
    FROM numbers(10)
  )
SELECT * FROM test
ORDER BY toString(number);
