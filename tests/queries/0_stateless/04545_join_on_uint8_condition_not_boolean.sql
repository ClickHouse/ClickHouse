-- The ON-section condition column is only boolean-like: any non-zero byte means the row
-- passes. Use values > 1 to catch code that treats the mask as strictly 0/1.

SELECT '-- plain keys, condition values 0/1/2';
SELECT countIf(r.k IS NOT NULL), countIf(l.cond = 2 AND r.k IS NOT NULL)
FROM (SELECT number % 100 AS k, toUInt8(number % 3) AS cond FROM numbers(1000)) l
LEFT JOIN (SELECT number AS k FROM numbers(50)) r ON l.k = r.k AND l.cond
SETTINGS join_algorithm = 'hash', join_use_nulls = 1;

SELECT '-- nullable keys, condition values 0/1/2';
SELECT countIf(r.k IS NOT NULL), countIf(l.cond = 2 AND r.k IS NOT NULL)
FROM (SELECT if(number % 7 = 0, NULL, number % 100) AS k, toUInt8(number % 3) AS cond FROM numbers(1000)) l
LEFT JOIN (SELECT toNullable(number) AS k FROM numbers(50)) r ON l.k = r.k AND l.cond
SETTINGS join_algorithm = 'hash', join_use_nulls = 1;

SELECT '-- inner join, condition values 0/1/2';
SELECT count()
FROM (SELECT number % 100 AS k, toUInt8(number % 3) AS cond FROM numbers(1000)) l
INNER JOIN (SELECT number AS k FROM numbers(50)) r ON l.k = r.k AND l.cond
SETTINGS join_algorithm = 'hash';

SELECT '-- parallel_hash, condition values 0/1/2';
SELECT countIf(r.k IS NOT NULL)
FROM (SELECT number % 100 AS k, toUInt8(number % 3) AS cond FROM numbers(1000)) l
LEFT JOIN (SELECT number AS k FROM numbers(50)) r ON l.k = r.k AND l.cond
SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1;

SELECT '-- continuation chunks: OR disjuncts, nullable keys, mask, tiny max_joined_block_size_rows';
SELECT count(), sum(cityHash64(coalesce(l.k, 0), coalesce(r.a, 0), coalesce(r.b, 0)) % 1000)
FROM (SELECT if(number % 7 = 0, NULL, number % 50) AS k, toUInt8(number % 3) AS cond FROM numbers(5000)) l
LEFT JOIN (SELECT toNullable(number % 25) AS a, toNullable(number % 30) AS b FROM numbers(300)) r
    ON (l.k = r.a AND l.cond) OR (l.k = r.b)
SETTINGS join_algorithm = 'hash', join_use_nulls = 1, max_joined_block_size_rows = 256;
