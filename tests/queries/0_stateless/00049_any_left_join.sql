SELECT number, joined FROM system.numbers ANY LEFT JOIN (SELECT number * 2 AS number, number * 10 + 1 AS joined FROM system.numbers LIMIT 10) js2 USING number LIMIT 10
