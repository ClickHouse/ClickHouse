-- Tags: no-backward-compatibility-check

-- Regression for PartialSortingTransform optimization that requires at least 1500 rows.
SELECT * FROM (SELECT * FROM (SELECT 0 a, toNullable(number) b, toString(number) c FROM numbers(1e6)) ORDER BY a DESC, b DESC, c LIMIT 1500) limit 10;
SELECT number FROM (SELECT number, 1 AS k FROM numbers(100000) ORDER BY k ASC, number DESC LIMIT 1025, 1023) LIMIT 5;
