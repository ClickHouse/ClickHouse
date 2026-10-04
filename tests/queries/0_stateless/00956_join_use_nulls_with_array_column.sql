SET join_use_nulls = 1;
-- The left side is bounded: the outer `LIMIT` counts joined rows, so it is not pushed into a source of a join.
SELECT number FROM numbers(10) AS t SEMI LEFT JOIN (SELECT number, ['test'] FROM system.numbers LIMIT 1) js2 USING (number) LIMIT 1;
SELECT number FROM numbers(10) AS t ANY LEFT  JOIN (SELECT number, ['test'] FROM system.numbers LIMIT 1) js2 USING (number) LIMIT 1;
