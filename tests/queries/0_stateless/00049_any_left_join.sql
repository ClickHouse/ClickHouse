-- The left side is bounded: the outer `LIMIT` counts joined rows, so it is not pushed into a source of a join.
SELECT * FROM (
SELECT number, joined FROM (SELECT number FROM system.numbers LIMIT 10) AS t ANY LEFT JOIN (SELECT number * 2 AS number, number * 10 + 1 AS joined FROM system.numbers LIMIT 10) js2 USING number LIMIT 10
) ORDER BY ALL;
