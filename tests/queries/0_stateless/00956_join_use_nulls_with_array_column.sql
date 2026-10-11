SET join_use_nulls = 1;
-- The left side is bounded: the outer `LIMIT` counts joined rows, so it is not pushed into a source of a join.
-- `ORDER BY` makes the result deterministic: without it, any row of the left side may come first (e.g. with parallel replicas).
SELECT number FROM numbers(10) AS t SEMI LEFT JOIN (SELECT number, ['test'] FROM system.numbers LIMIT 1) js2 USING (number) ORDER BY number LIMIT 1;
SELECT number FROM numbers(10) AS t ANY LEFT  JOIN (SELECT number, ['test'] FROM system.numbers LIMIT 1) js2 USING (number) ORDER BY number LIMIT 1;
