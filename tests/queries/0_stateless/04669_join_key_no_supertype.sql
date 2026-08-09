-- JOIN keys of types that have no least supertype, such as `UInt64` and `Int64`.
-- It is enough to convert both keys to the type of the values they have in common.
-- https://github.com/ClickHouse/ClickHouse/issues/21794

-- It is implemented only in the analyzer; with `enable_analyzer = 0` such a query is still rejected.
SET enable_analyzer = 1;

SELECT 'The query from the issue';
SELECT *
FROM (SELECT number FROM numbers(10)) AS a
INNER JOIN (SELECT number FROM numbers(10)) AS b ON a.number = b.number - 1
ORDER BY ALL;

DROP TABLE IF EXISTS t_unsigned;
DROP TABLE IF EXISTS t_signed;

CREATE TABLE t_unsigned (x UInt64) ENGINE = Memory;
CREATE TABLE t_signed (y Int64) ENGINE = Memory;

INSERT INTO t_unsigned VALUES (0), (1), (9223372036854775807), (9223372036854775808), (18446744073709551615);
INSERT INTO t_signed VALUES (-9223372036854775808), (-1), (0), (1), (9223372036854775807);

SELECT 'INNER';
SELECT x, y FROM t_unsigned INNER JOIN t_signed ON x = y ORDER BY ALL;

SELECT 'LEFT';
SELECT x, y FROM t_unsigned LEFT JOIN t_signed ON x = y ORDER BY ALL;

SELECT 'RIGHT';
SELECT x, y FROM t_unsigned RIGHT JOIN t_signed ON x = y ORDER BY ALL;

SELECT 'FULL';
SELECT x, y FROM t_unsigned FULL JOIN t_signed ON x = y ORDER BY ALL;

SELECT 'The types of the result are not affected';
SELECT toTypeName(x), toTypeName(y) FROM t_unsigned INNER JOIN t_signed ON x = y LIMIT 1;

SELECT 'The same for every JOIN algorithm';
SELECT count() FROM t_unsigned INNER JOIN t_signed ON x = y SETTINGS join_algorithm = 'hash';
SELECT count() FROM t_unsigned INNER JOIN t_signed ON x = y SETTINGS join_algorithm = 'parallel_hash';
SELECT count() FROM t_unsigned INNER JOIN t_signed ON x = y SETTINGS join_algorithm = 'grace_hash';
SELECT count() FROM t_unsigned INNER JOIN t_signed ON x = y SETTINGS join_algorithm = 'partial_merge';
SELECT count() FROM t_unsigned INNER JOIN t_signed ON x = y SETTINGS join_algorithm = 'full_sorting_merge';

SELECT 'An expression as a key';
SELECT x, y FROM t_unsigned INNER JOIN t_signed ON x = -y ORDER BY ALL;

SELECT 'Multiple keys';
SELECT x, y FROM t_unsigned INNER JOIN t_signed ON x = y AND x + 1 = y + 1 ORDER BY ALL;

SELECT 'A narrower signed type: only the values from 0 to 127 can match';
SELECT x, y FROM t_unsigned INNER JOIN (SELECT CAST(number, 'Int8') AS y FROM numbers(200)) AS t ON x = y ORDER BY ALL;

SELECT 'The widest types';
SELECT * FROM (SELECT CAST(18446744073709551615, 'UInt256') AS x) AS a
INNER JOIN (SELECT CAST(18446744073709551615, 'Int256') AS y) AS b ON a.x = b.y;

SELECT 'Nullable and LowCardinality keys';
SELECT * FROM (SELECT CAST(1, 'Nullable(UInt64)') AS x) AS a
INNER JOIN (SELECT CAST(1, 'Int64') AS y) AS b ON a.x = b.y;
SELECT * FROM (SELECT CAST(1, 'LowCardinality(UInt64)') AS x) AS a
INNER JOIN (SELECT CAST(1, 'Int64') AS y) AS b ON a.x = b.y
SETTINGS allow_suspicious_low_cardinality_types = 1;

SELECT 'A Tuple key: an element out of the common range turns the whole tuple into NULL, and it does not match anything';
SELECT * FROM (SELECT tuple(number) AS t FROM numbers(3)) AS a
INNER JOIN (SELECT tuple(number - 1) AS t FROM numbers(3)) AS b ON a.t = b.t
ORDER BY ALL;
SELECT * FROM (SELECT tuple(CAST(18446744073709551615, 'UInt64')) AS t) AS a
INNER JOIN (SELECT tuple(CAST(-1, 'Int64')) AS t) AS b ON a.t = b.t;
SELECT t, toTypeName(t) FROM (SELECT tuple(CAST(1, 'UInt64')) AS t) AS a
INNER JOIN (SELECT tuple(CAST(1, 'Int64')) AS t) AS b USING (t);

SELECT 'A Tuple with an element that accurateCastOrNull cannot represent, such as an Array or a Map, is rejected';
SELECT * FROM (SELECT tuple([CAST(1, 'UInt64')]) AS t) AS a
INNER JOIN (SELECT tuple([CAST(1, 'Int64')]) AS t) AS b ON a.t = b.t; -- { serverError NO_COMMON_TYPE }
SELECT * FROM (SELECT tuple([CAST(1, 'UInt64')]) AS t) AS a
INNER JOIN (SELECT tuple([CAST(1, 'Int64')]) AS t) AS b USING (t); -- { serverError NO_COMMON_TYPE }
SELECT * FROM (SELECT tuple(map('k', CAST(1, 'UInt64'))) AS t) AS a
INNER JOIN (SELECT tuple(map('k', CAST(1, 'Int64'))) AS t) AS b USING (t); -- { serverError NO_COMMON_TYPE }
SELECT * FROM (SELECT [CAST(1, 'UInt64')] AS t) AS a
INNER JOIN (SELECT [CAST(1, 'Int64')] AS t) AS b ON a.t = b.t; -- { serverError NO_COMMON_TYPE }

SELECT 'USING is supported for INNER JOIN, where the result contains only the common values';
SELECT x, toTypeName(x) FROM t_unsigned INNER JOIN (SELECT y AS x FROM t_signed) AS t USING (x) ORDER BY ALL;
SELECT x FROM t_unsigned INNER JOIN (SELECT y AS x FROM t_signed) AS t USING (x) ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge';

SELECT 'A USING key that is already Nullable still requires an accurate conversion';
SELECT * FROM (SELECT CAST(18446744073709551615, 'Nullable(UInt64)') AS x) AS a
INNER JOIN (SELECT CAST(-1, 'Int64') AS x) AS b USING (x);
SELECT x, toTypeName(x) FROM (SELECT CAST(9223372036854775807, 'Nullable(UInt64)') AS x) AS a
INNER JOIN (SELECT CAST(9223372036854775807, 'Int64') AS x) AS b USING (x);

SELECT 'SEMI JOIN keeps only the matched rows of the preserved side, so USING is supported for it as well';
SELECT x, toTypeName(x) FROM t_unsigned LEFT SEMI JOIN (SELECT y AS x FROM t_signed) AS t USING (x) ORDER BY ALL;
SELECT x, toTypeName(x) FROM t_unsigned RIGHT SEMI JOIN (SELECT y AS x FROM t_signed) AS t USING (x) ORDER BY ALL;

SELECT 'ANTI JOIN keeps exactly the unmatched rows, which may be out of the common range';
SELECT x FROM t_unsigned LEFT ANTI JOIN (SELECT y AS x FROM t_signed) AS t USING (x); -- { serverError NO_COMMON_TYPE }
SELECT x FROM t_unsigned RIGHT ANTI JOIN (SELECT y AS x FROM t_signed) AS t USING (x); -- { serverError NO_COMMON_TYPE }

SELECT 'For the other kinds of JOIN the result also contains the values that are out of the common range';
SELECT x FROM t_unsigned LEFT JOIN (SELECT y AS x FROM t_signed) AS t USING (x); -- { serverError NO_COMMON_TYPE }
SELECT x FROM t_unsigned RIGHT JOIN (SELECT y AS x FROM t_signed) AS t USING (x); -- { serverError NO_COMMON_TYPE }
SELECT x FROM t_unsigned FULL JOIN (SELECT y AS x FROM t_signed) AS t USING (x); -- { serverError NO_COMMON_TYPE }

SELECT 'A null-safe comparison cannot be done this way, because NULL matches NULL there';
SELECT * FROM t_unsigned INNER JOIN t_signed ON x IS NOT DISTINCT FROM y; -- { serverError NO_COMMON_TYPE, ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'The ASOF inequality needs the order of the values, not only the equality';
SELECT * FROM (SELECT x, 1 AS k FROM t_unsigned) AS a ASOF JOIN (SELECT y, 1 AS k FROM t_signed) AS b ON a.k = b.k AND a.x > b.y; -- { serverError NO_COMMON_TYPE }

SELECT 'In ASOF JOIN USING only the last column needs the order of the values, the preceding ones are equality keys';
SELECT * FROM (SELECT CAST(1, 'UInt64') AS k, 3 AS t) AS a
ASOF JOIN (SELECT CAST(1, 'Int64') AS k, 2 AS t) AS b USING (k, t);
SELECT * FROM (SELECT CAST(18446744073709551615, 'UInt64') AS k, 3 AS t) AS a
ASOF JOIN (SELECT CAST(-1, 'Int64') AS k, 2 AS t) AS b USING (k, t);
SELECT * FROM (SELECT 1 AS k, CAST(3, 'UInt64') AS t) AS a
ASOF JOIN (SELECT 1 AS k, CAST(2, 'Int64') AS t) AS b USING (k, t); -- { serverError NO_COMMON_TYPE }

SELECT 'The Join table engine holds a hash table prebuilt over the original key, so only the probe side can be converted';
DROP TABLE IF EXISTS t_join_unsigned;
DROP TABLE IF EXISTS t_join_signed;
CREATE TABLE t_join_unsigned (x UInt64, v String) ENGINE = Join(SEMI, LEFT, x);
CREATE TABLE t_join_signed (x Int64, v String) ENGINE = Join(SEMI, LEFT, x);
INSERT INTO t_join_unsigned VALUES (1, 'a'), (18446744073709551615, 'b');
INSERT INTO t_join_signed VALUES (1, 'a'), (-1, 'b');

SELECT 'The stored key is the common subtype: the probe key is converted, and an out-of-range value does not match anything';
SELECT * FROM (SELECT CAST(1, 'Int64') AS x) AS t SEMI LEFT JOIN t_join_unsigned USING (x);
SELECT * FROM (SELECT CAST(-1, 'Int64') AS x) AS t SEMI LEFT JOIN t_join_unsigned USING (x);

SELECT 'The stored key is not the common subtype: the prebuilt hash table cannot be converted';
SELECT * FROM (SELECT CAST(1, 'UInt64') AS x) AS t SEMI LEFT JOIN t_join_signed USING (x); -- { serverError NO_COMMON_TYPE }
SELECT * FROM (SELECT CAST(1, 'UInt64') AS x) AS t SEMI LEFT JOIN t_join_signed AS j ON t.x = j.x; -- { serverError NO_COMMON_TYPE }

DROP TABLE t_join_unsigned;
DROP TABLE t_join_signed;

DROP TABLE t_unsigned;
DROP TABLE t_signed;
