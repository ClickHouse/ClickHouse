-- https://github.com/ClickHouse/ClickHouse/issues/25655
SET enable_analyzer=1;
SELECT
  sum(t.b) / 1 a,
  sum(t.a)
FROM ( SELECT 1 a, 2 b ) t;
