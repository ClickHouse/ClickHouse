-- https://github.com/ClickHouse/ClickHouse/pull/40065
SET enable_analyzer=1;

SELECT
(
  SELECT
      1 AS number,
      number
  FROM numbers(1)
) AS s,
(
  SELECT
      1,
      number
  FROM numbers(1)
) AS s2;

SELECT
(
  SELECT
          1 + 2 AS number,
          1 + number AS b
  FROM system.numbers
  LIMIT 10, 1
),
(
  SELECT
          1 + 2 AS number2,
          1 + number AS b
  FROM system.numbers
  LIMIT 10, 1
);
