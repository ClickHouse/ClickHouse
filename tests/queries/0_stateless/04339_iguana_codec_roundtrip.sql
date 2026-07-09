-- Tags: no-fasttest
-- ^ the Iguana codec lives in a contrib library that is not built in fast test.

SELECT '-- experimental gate (allow_experimental_codecs defaults to 0)';
DROP TABLE IF EXISTS iguana_gate;
-- Without the setting the experimental codec must be rejected.
CREATE TABLE iguana_gate (s String CODEC(Iguana)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SET allow_experimental_codecs = 1;

SELECT '-- system.codecs';
SELECT name, is_compression, is_generic_compression, is_experimental
FROM system.codecs WHERE name = 'Iguana';

SELECT '-- String round-trip (skewed -> ANS, random -> raw fallback, plus edge cases)';
DROP TABLE IF EXISTS s_none;
DROP TABLE IF EXISTS s_iguana;
CREATE TABLE s_none   (id UInt64, s String CODEC(NONE))   ENGINE = MergeTree ORDER BY id;
CREATE TABLE s_iguana (id UInt64, s String CODEC(Iguana)) ENGINE = MergeTree ORDER BY id;

-- Skewed/text-like data: long, low-entropy strings that the entropy coder should shrink.
INSERT INTO s_none SELECT number, repeat(['aaaa','abc ','hello ','xy'][number % 4 + 1], number % 50 + 1) FROM numbers(20000);
-- Random high-entropy strings: forces the verbatim fallback path.
INSERT INTO s_none SELECT number + 100000, randomString(number % 200) FROM numbers(20000);
-- Edge cases: empty strings and a single repeated character.
INSERT INTO s_none SELECT number + 300000, if(number % 2 = 0, '', repeat('Z', number % 300)) FROM numbers(5000);

INSERT INTO s_iguana SELECT * FROM s_none;
OPTIMIZE TABLE s_iguana FINAL;

SELECT count() FROM s_none;
SELECT count() FROM s_iguana;
-- Exact row-by-row comparison: must be 0 mismatches.
SELECT count() FROM s_none AS a INNER JOIN s_iguana AS b USING (id) WHERE a.s != b.s;
-- Multiset checksum cross-check (order independent).
SELECT sum(cityHash64(s)) = (SELECT sum(cityHash64(s)) FROM s_none) FROM s_iguana;

SELECT '-- Numeric round-trip with plain Iguana and Delta+Iguana';
DROP TABLE IF EXISTS n_none;
DROP TABLE IF EXISTS n_iguana;
CREATE TABLE n_none   (id UInt64, u8 UInt8 CODEC(NONE), i64 Int64 CODEC(NONE), d Date CODEC(NONE))
    ENGINE = MergeTree ORDER BY id;
CREATE TABLE n_iguana (id UInt64, u8 UInt8 CODEC(Iguana), i64 Int64 CODEC(Delta, Iguana), d Date CODEC(Iguana))
    ENGINE = MergeTree ORDER BY id;

INSERT INTO n_none SELECT number, number % 7, intDiv(number, 3) - 50000, toDate('2020-01-01') + (number % 4000) FROM numbers(100000);
INSERT INTO n_iguana SELECT * FROM n_none;
OPTIMIZE TABLE n_iguana FINAL;

SELECT count() FROM n_none AS a INNER JOIN n_iguana AS b USING (id)
    WHERE a.u8 != b.u8 OR a.i64 != b.i64 OR a.d != b.d;
-- Independent aggregate cross-check: sums over both tables must be equal.
SELECT (SELECT sum(u8) FROM n_none)  = (SELECT sum(u8) FROM n_iguana),
       (SELECT sum(i64) FROM n_none) = (SELECT sum(i64) FROM n_iguana),
       (SELECT sum(toUInt32(d)) FROM n_none) = (SELECT sum(toUInt32(d)) FROM n_iguana);

SELECT '-- Nullable and FixedString round-trip';
DROP TABLE IF EXISTS f_none;
DROP TABLE IF EXISTS f_iguana;
CREATE TABLE f_none   (id UInt64, fs FixedString(16) CODEC(NONE),   ns Nullable(String) CODEC(NONE))   ENGINE = MergeTree ORDER BY id;
CREATE TABLE f_iguana (id UInt64, fs FixedString(16) CODEC(Iguana), ns Nullable(String) CODEC(Iguana)) ENGINE = MergeTree ORDER BY id;

INSERT INTO f_none SELECT number, toFixedString(toString(number % 100), 16), if(number % 3 = 0, NULL, repeat('q', number % 40)) FROM numbers(50000);
INSERT INTO f_iguana SELECT * FROM f_none;
OPTIMIZE TABLE f_iguana FINAL;

SELECT count() FROM f_none AS a INNER JOIN f_iguana AS b USING (id)
    WHERE a.fs != b.fs OR (a.ns != b.ns) OR (a.ns IS NULL) != (b.ns IS NULL);
SELECT (SELECT countIf(ns IS NULL) FROM f_none) = (SELECT countIf(ns IS NULL) FROM f_iguana);

DROP TABLE IF EXISTS iguana_gate;
DROP TABLE s_none;
DROP TABLE s_iguana;
DROP TABLE n_none;
DROP TABLE n_iguana;
DROP TABLE f_none;
DROP TABLE f_iguana;
