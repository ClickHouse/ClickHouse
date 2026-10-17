-- `numbers`-like sources used to receive the outer `LIMIT` as a row budget even when the query has a
-- `JOIN`. The `LIMIT` counts joined rows, so a join that drops the first rows of the source then lost
-- the rows that match later: every query below returned nothing instead of one row. The unbounded
-- `system.numbers` and `system.primes` are bounded by a `WHERE` range, which still let the hint through.

SELECT n.number FROM numbers(100) AS n INNER JOIN (SELECT 50 AS x) AS t ON n.number = t.x LIMIT 1;
SELECT n.number FROM (SELECT 50 AS x) AS t INNER JOIN numbers(100) AS n ON n.number = t.x LIMIT 1;
SELECT n.number FROM system.numbers AS n INNER JOIN (SELECT 50 AS x) AS t ON n.number = t.x WHERE n.number < 100 LIMIT 1;
SELECT n.number FROM numbers(100) AS n, (SELECT 50 AS x) AS t WHERE n.number = t.x LIMIT 1;
SELECT n.number FROM numbers(100) AS n LEFT JOIN (SELECT 50 AS x) AS t ON n.number = t.x WHERE t.x = 50 LIMIT 1;
SELECT n.number FROM numbers(100) AS n INNER JOIN (SELECT 50 AS x) AS t ON n.number = t.x LIMIT 1 OFFSET 0;
SELECT n.generate_series FROM generate_series(0, 99) AS n INNER JOIN (SELECT 50 AS x) AS t ON n.generate_series = t.x LIMIT 1;
SELECT n.prime FROM system.primes AS n INNER JOIN (SELECT 97 AS x) AS t ON n.prime = t.x WHERE n.prime < 1000 LIMIT 1;
SELECT n.prime FROM primes(100) AS n INNER JOIN (SELECT 97 AS x) AS t ON n.prime = t.x LIMIT 1;
