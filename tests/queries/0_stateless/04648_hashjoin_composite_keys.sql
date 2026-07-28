-- Composite and mixed-type keys that map to `HashJoin::Type::hashed`, where the SipHash128
-- digest of the key columns is the hash map key. Also covers composite IN, whose `Set` uses
-- the same `HashMethodHashed` key getter.

SELECT '-- two String keys, hash';
SELECT count(), sum(length(r.s2))
FROM (SELECT toString(number % 3000) AS s1, toString(number % 3000) AS s2 FROM numbers(10000)) AS l
INNER JOIN (SELECT toString(number) AS s1, toString(number) AS s2 FROM numbers(2000)) AS r
    ON l.s1 = r.s1 AND l.s2 = r.s2
SETTINGS join_algorithm = 'hash';

SELECT '-- two String keys, parallel_hash';
SELECT count(), sum(length(r.s2))
FROM (SELECT toString(number % 3000) AS s1, toString(number % 3000) AS s2 FROM numbers(10000)) AS l
INNER JOIN (SELECT toString(number) AS s1, toString(number) AS s2 FROM numbers(2000)) AS r
    ON l.s1 = r.s1 AND l.s2 = r.s2
SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1;

SELECT '-- mixed OR disjuncts, duplicates, small output blocks';
SELECT count(), sum(r.i)
FROM (SELECT number % 300 AS i, toString(number % 300) AS s FROM numbers(3000)) AS l
INNER JOIN (SELECT number % 200 AS i, toString(number % 200) AS s FROM numbers(2000)) AS r
    ON l.i = r.i OR l.s = r.s
SETTINGS max_joined_block_size_rows = 1024;

SELECT '-- RIGHT JOIN, two String keys, parallel_hash';
SELECT count(), countIf(l.s1 = ''), sum(length(r.s2))
FROM (SELECT toString(number % 1000) AS s1, toString(number % 1000) AS s2 FROM numbers(5000)) AS l
RIGHT JOIN (SELECT toString(number) AS s1, toString(number) AS s2 FROM numbers(2000)) AS r
    ON l.s1 = r.s1 AND l.s2 = r.s2
SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1;

SELECT '-- Nullable key in composite';
SELECT count(), sum(r.i)
FROM (SELECT nullIf(number % 300, 42) AS i, toString(number % 300) AS s FROM numbers(10000)) AS l
INNER JOIN (SELECT toNullable(number) AS i, toString(number) AS s FROM numbers(200)) AS r
    ON l.i = r.i AND l.s = r.s
SETTINGS join_algorithm = 'hash';

SELECT '-- LowCardinality String in composite';
SELECT count(), sum(r.i)
FROM (SELECT number % 300 AS i, toLowCardinality(toString(number % 300)) AS s FROM numbers(10000)) AS l
INNER JOIN (SELECT number AS i, toLowCardinality(toString(number)) AS s FROM numbers(200)) AS r
    ON l.i = r.i AND l.s = r.s
SETTINGS join_algorithm = 'hash';

SELECT '-- ASOF with mixed equality prefix';
SELECT count(), sum(r.t)
FROM (SELECT number % 100 AS i, toString(number % 100) AS s, number AS t FROM numbers(2000)) AS l
ASOF INNER JOIN (SELECT number % 100 AS i, toString(number % 100) AS s, number * 2 AS t FROM numbers(200)) AS r
    ON l.i = r.i AND l.s = r.s AND l.t >= r.t
SETTINGS join_algorithm = 'hash';

SELECT '-- composite IN (Set)';
SELECT count()
FROM numbers(10000)
WHERE (number % 3000, toString(number % 3000)) IN (SELECT number, toString(number) FROM numbers(2000));
