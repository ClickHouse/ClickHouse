DROP TABLE IF EXISTS t_enum_key;
DROP TABLE IF EXISTS t_enum_nokey;

CREATE TABLE t_enum_key (e Enum8('a' = 1, 'b' = 2), v UInt8) ENGINE = MergeTree ORDER BY e;
INSERT INTO t_enum_key VALUES ('a', 1), ('b', 2);

CREATE TABLE t_enum_nokey (e Enum16('a' = 1, 'b' = 2), v UInt8) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_enum_nokey VALUES ('a', 1), ('b', 2);

SET validate_enum_literals_in_operators = 0;

SELECT 'key column';
SELECT count() FROM t_enum_key WHERE e = 'a';
SELECT count() FROM t_enum_key WHERE e = 'c';
SELECT count() FROM t_enum_key WHERE 'c' = e;
SELECT count() FROM t_enum_key WHERE e != 'c';
SELECT count() FROM t_enum_key WHERE 'c' != e;
SELECT count() FROM t_enum_key WHERE e IN ('c');
SELECT count() FROM t_enum_key WHERE e IN ('a', 'c');
SELECT count() FROM t_enum_key WHERE e = 'c' OR v = 2;
SELECT count() FROM t_enum_key WHERE e != 'c' AND v = 2;
SELECT count() FROM t_enum_key WHERE NOT (e = 'c');
SELECT count() FROM t_enum_key WHERE e < 'c'; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_enum_key WHERE e >= 'c'; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

SELECT 'non-key column';
SELECT count() FROM t_enum_nokey WHERE e = 'c';
SELECT count() FROM t_enum_nokey WHERE e != 'c';
SELECT count() FROM t_enum_nokey WHERE e < 'c'; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_enum_nokey WHERE e >= 'c'; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

SELECT 'bloom filter index';
DROP TABLE IF EXISTS t_enum_bf;
CREATE TABLE t_enum_bf (e Enum8('a' = 1, 'b' = 2), arr Array(Enum8('a' = 1, 'b' = 2)), v UInt8,
    INDEX bf_e e TYPE bloom_filter GRANULARITY 1, INDEX bf_arr arr TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY v;
INSERT INTO t_enum_bf VALUES ('a', ['a'], 1), ('b', ['b'], 2);
SELECT count() FROM t_enum_bf WHERE e = 'c';
SELECT count() FROM t_enum_bf WHERE e != 'c';
SELECT count() FROM t_enum_bf WHERE NOT (e = 'c');
SELECT count() FROM t_enum_bf PREWHERE e = 'c';
SELECT count() FROM t_enum_bf WHERE e = 'c' OR v = 2;
SELECT count() FROM t_enum_bf ARRAY JOIN arr AS x WHERE x = 'c';
SELECT count() FROM t_enum_bf WHERE e IN ('c');
SELECT count() FROM t_enum_bf WHERE e IN ('a', 'c');
SELECT count() FROM t_enum_bf WHERE arrayJoin(arr) IN ('c');

SELECT 'nullable key';
DROP TABLE IF EXISTS t_enum_null;
CREATE TABLE t_enum_null (e Nullable(Enum8('a' = 1, 'b' = 2)), v UInt8) ENGINE = MergeTree ORDER BY e SETTINGS allow_nullable_key = 1;
INSERT INTO t_enum_null VALUES ('a', 1), (NULL, 2);
SELECT count() FROM t_enum_null WHERE e = 'c';
SELECT count() FROM t_enum_null WHERE e != 'c';

SELECT 'partition key';
DROP TABLE IF EXISTS t_enum_part;
CREATE TABLE t_enum_part (e Enum8('a' = 1, 'b' = 2), v UInt8) ENGINE = MergeTree PARTITION BY e ORDER BY v;
INSERT INTO t_enum_part VALUES ('a', 1), ('b', 2);
SELECT count() FROM t_enum_part WHERE e = 'c';
SELECT count() FROM t_enum_part WHERE e != 'c';

SET validate_enum_literals_in_operators = 1;

SELECT count() FROM t_enum_key WHERE e = 'c'; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_enum_key WHERE e != 'c'; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_enum_key WHERE e IN ('c'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_enum_nokey WHERE e = 'c'; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_enum_nokey WHERE e != 'c'; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT count() FROM t_enum_bf WHERE e = 'c'; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

DROP TABLE t_enum_key;
DROP TABLE t_enum_nokey;
DROP TABLE t_enum_bf;
DROP TABLE t_enum_null;
DROP TABLE t_enum_part;
