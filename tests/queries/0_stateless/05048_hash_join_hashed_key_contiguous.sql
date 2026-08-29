-- A join on several key columns, or on a single key column with no fixed-width representation,
-- uses the `hashed` method, where the key is a 128-bit hash of the key columns of the row. The
-- hash is computed from the row laid out contiguously when every key column serializes to the
-- bytes it hashes, so keys whose pieces could be told apart only by their lengths, keys wider
-- than the layout is used for, and keys stored differently on the two sides all have to join
-- exactly as they did before.

SET join_algorithm = 'hash', max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (a String, b String, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_right (a String, b String, w UInt64) ENGINE = MergeTree ORDER BY tuple();

-- Splitting the same characters differently between the two key columns must not join.
INSERT INTO t_left VALUES ('a', 'bc', 1), ('ab', 'c', 2), ('abc', '', 3), ('', 'abc', 4), ('', '', 5);
INSERT INTO t_right VALUES ('ab', 'c', 20), ('', '', 50), ('a', 'bcd', 60);

SELECT 'split keys';
SELECT l.a, l.b, l.v, r.w FROM t_left AS l INNER JOIN t_right AS r ON l.a = r.a AND l.b = r.b ORDER BY l.v;

-- A zero byte inside a key is data, not a terminator.
TRUNCATE TABLE t_left;
TRUNCATE TABLE t_right;
INSERT INTO t_left VALUES ('a\0b', 'c', 1), ('a', '\0bc', 2), ('a\0', 'bc', 3);
INSERT INTO t_right VALUES ('a\0b', 'c', 10), ('a', '\0bc', 20);

SELECT 'zero bytes in keys';
SELECT l.v, r.w FROM t_left AS l INNER JOIN t_right AS r ON l.a = r.a AND l.b = r.b ORDER BY l.v;

-- Keys wider than the row layout is used for take the same route as narrow ones.
TRUNCATE TABLE t_left;
TRUNCATE TABLE t_right;
INSERT INTO t_left SELECT leftPad(toString(number), 400, 'x'), leftPad(toString(number), 400, 'y'), number FROM numbers(100);
INSERT INTO t_right SELECT leftPad(toString(number), 400, 'x'), leftPad(toString(number), 400, 'y'), number FROM numbers(50);

SELECT 'wide keys';
SELECT count(), sum(l.v), sum(r.w) FROM t_left AS l INNER JOIN t_right AS r ON l.a = r.a AND l.b = r.b;

DROP TABLE t_left;
DROP TABLE t_right;

-- The two sides of a join may hold the same key values in different physical representations,
-- and both sides have to arrive at the same hash for a row to be found.
DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_low_cardinality;
DROP TABLE IF EXISTS t_nullable;

CREATE TABLE t_plain (a String, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_low_cardinality (a LowCardinality(String), b UInt64, w UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_nullable (a Nullable(String), b Nullable(UInt64), w UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_plain SELECT toString(number % 7), number % 5, number FROM numbers(100);
INSERT INTO t_low_cardinality SELECT toString(number % 7), number % 5, number FROM numbers(100);
INSERT INTO t_nullable SELECT if(number % 11 = 0, NULL, toString(number % 7)), if(number % 13 = 0, NULL, number % 5), number FROM numbers(100);

SELECT 'low cardinality against plain';
SELECT count(), sum(l.v), sum(r.w) FROM t_plain AS l INNER JOIN t_low_cardinality AS r ON l.a = r.a AND l.b = r.b;

SELECT 'nullable against plain';
SELECT count(), sum(l.v), sum(r.w) FROM t_plain AS l INNER JOIN t_nullable AS r ON l.a = r.a AND l.b = r.b;

SELECT 'nullable against nullable';
SELECT count(), sum(l.w), sum(r.w) FROM t_nullable AS l INNER JOIN t_nullable AS r ON l.a = r.a AND l.b = r.b;

-- Keys that are not laid out contiguously keep working as well.
SELECT 'array and tuple keys';
SELECT count() FROM t_plain AS l INNER JOIN t_plain AS r ON [l.a] = [r.a] AND (l.b, l.a) = (r.b, r.a);

DROP TABLE t_plain;
DROP TABLE t_low_cardinality;
DROP TABLE t_nullable;

-- Compare against an algorithm that does not hash the key at all, over keys that are easy to
-- confuse with one another: same characters split differently, empty parts, zero bytes, and
-- values long enough to fall outside the contiguous layout.
DROP TABLE IF EXISTS t_tricky_left;
DROP TABLE IF EXISTS t_tricky_right;

CREATE TABLE t_tricky_left (a String, b Nullable(String), c LowCardinality(String), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_tricky_right (a String, b Nullable(String), c LowCardinality(String), w UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_tricky_left SELECT
    repeat('a', number % 5) || '\0' || toString(number % 37),
    if(number % 17 = 0, NULL, repeat('b', number % 600)),
    toString(number % 11),
    number
FROM numbers(20000);

INSERT INTO t_tricky_right SELECT
    repeat('a', number % 6) || '\0' || toString(number % 41),
    if(number % 19 = 0, NULL, repeat('b', number % 600)),
    toString(number % 13),
    number
FROM numbers(20000);

SELECT 'same result as full sorting merge';
SELECT
(
    SELECT sum(cityHash64(l.v, r.w)) FROM t_tricky_left AS l INNER JOIN t_tricky_right AS r ON l.a = r.a AND l.b = r.b AND l.c = r.c
    SETTINGS join_algorithm = 'hash'
) = (
    SELECT sum(cityHash64(l.v, r.w)) FROM t_tricky_left AS l INNER JOIN t_tricky_right AS r ON l.a = r.a AND l.b = r.b AND l.c = r.c
    SETTINGS join_algorithm = 'full_sorting_merge'
);

-- The same for the rows that find nothing, which full sorting merge only does as a left join.
SELECT 'anti join same result as full sorting merge';
SELECT
(
    SELECT sum(l.v) FROM t_tricky_left AS l LEFT ANTI JOIN t_tricky_right AS r ON l.a = r.a AND l.b = r.b AND l.c = r.c
    SETTINGS join_algorithm = 'hash'
) = (
    SELECT sum(l.v) FROM t_tricky_left AS l LEFT JOIN t_tricky_right AS r ON l.a = r.a AND l.b = r.b AND l.c = r.c
    WHERE r.a IS NULL
    SETTINGS join_algorithm = 'full_sorting_merge', join_use_nulls = 1
);

DROP TABLE t_tricky_left;
DROP TABLE t_tricky_right;

-- The same key layout is used wherever a row of several columns is hashed into a 128-bit key:
-- IN sets, DISTINCT, and the array functions that count distinct tuples.
DROP TABLE IF EXISTS t_keys;

CREATE TABLE t_keys (a String, b String, c Nullable(String), v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_keys SELECT
    repeat('a', number % 4) || '\0' || toString(number % 23),
    repeat('b', number % 3),
    if(number % 7 = 0, NULL, toString(number % 5)),
    number
FROM numbers(10000);

SELECT 'in set';
SELECT count() FROM t_keys WHERE (a, b, c) IN (SELECT a, b, c FROM t_keys WHERE v % 3 = 0);

SELECT 'distinct matches uniqExact';
SELECT (SELECT count() FROM (SELECT DISTINCT a, b, c FROM t_keys)) = (SELECT uniqExact((a, b, c)) FROM t_keys);

SELECT 'array uniq matches array distinct';
SELECT sum(arrayUniq(x, y) = length(arrayDistinct(arrayMap((p, q) -> (p, q), x, y))))
FROM (SELECT groupArray(a) AS x, groupArray(b) AS y FROM t_keys GROUP BY v % 97);

SELECT 'array enumerate uniq';
SELECT arrayEnumerateUniq(['a\0', 'a', 'a\0'], ['', 'b\0', '']);

DROP TABLE t_keys;

-- The probe loop starts the load of the hash-table cell a row will land in some rows before it
-- reaches that row. It does so only once the build side is past the cache, and for three key
-- methods: `hashed` here, and a single `String` or `FixedString` key. Whether the loop runs ahead
-- of itself must not change a single row it finds.
DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_build;

CREATE TABLE t_probe (a String, b String, f FixedString(24), v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_build (a String, b String, f FixedString(24), w UInt64) ENGINE = MergeTree ORDER BY tuple();

-- Enough distinct keys that the hash table is larger than the cache the prefetch is there to miss.
INSERT INTO t_build SELECT
    leftPad(toString(number), 24, '0'),
    leftPad(toString(number % 331), 12, '0'),
    toFixedString(leftPad(toString(number), 24, '0'), 24),
    number
FROM numbers(300000);

INSERT INTO t_probe SELECT
    leftPad(toString(number % 400000), 24, '0'),
    leftPad(toString(number % 331), 12, '0'),
    toFixedString(leftPad(toString(number % 400000), 24, '0'), 24),
    number
FROM numbers(600000);

SELECT 'prefetching the probe finds the same rows';
SELECT
(
    SELECT sum(cityHash64(l.v, r.w)) FROM t_probe AS l INNER JOIN t_build AS r ON l.a = r.a AND l.b = r.b
    SETTINGS enable_software_prefetch_in_join = 1
) = (
    SELECT sum(cityHash64(l.v, r.w)) FROM t_probe AS l INNER JOIN t_build AS r ON l.a = r.a AND l.b = r.b
    SETTINGS enable_software_prefetch_in_join = 0
) AS hashed_key,
(
    SELECT sum(cityHash64(l.v, r.w)) FROM t_probe AS l INNER JOIN t_build AS r ON l.a = r.a
    SETTINGS enable_software_prefetch_in_join = 1
) = (
    SELECT sum(cityHash64(l.v, r.w)) FROM t_probe AS l INNER JOIN t_build AS r ON l.a = r.a
    SETTINGS enable_software_prefetch_in_join = 0
) AS string_key,
(
    SELECT sum(cityHash64(l.v, r.w)) FROM t_probe AS l INNER JOIN t_build AS r ON l.f = r.f
    SETTINGS enable_software_prefetch_in_join = 1
) = (
    SELECT sum(cityHash64(l.v, r.w)) FROM t_probe AS l INNER JOIN t_build AS r ON l.f = r.f
    SETTINGS enable_software_prefetch_in_join = 0
) AS fixed_string_key;

SELECT 'and the same rows that are missing';
SELECT
(
    SELECT sum(l.v) FROM t_probe AS l LEFT ANTI JOIN t_build AS r ON l.a = r.a AND l.b = r.b
    SETTINGS enable_software_prefetch_in_join = 1
) = (
    SELECT sum(l.v) FROM t_probe AS l LEFT ANTI JOIN t_build AS r ON l.a = r.a AND l.b = r.b
    SETTINGS enable_software_prefetch_in_join = 0
);

DROP TABLE t_probe;
DROP TABLE t_build;
