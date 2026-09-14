-- Index analysis casts the set elements into the key column type, while runtime `IN` membership casts
-- the key into the set element type. Where the two conversions disagree, the set atom stands for
-- different key values than the predicate matches, and partition pruning then drops rows that do
-- satisfy it. Every cell compares a partitioned MergeTree against a Memory table holding the same rows,
-- so a `0` means the index changed the answer.

DROP TABLE IF EXISTS k_uint64;
DROP TABLE IF EXISTS k_uint64_mem;
CREATE TABLE k_uint64 (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO k_uint64 VALUES (1), (2);
CREATE TABLE k_uint64_mem (k UInt64) ENGINE = Memory;
INSERT INTO k_uint64_mem SELECT * FROM k_uint64;

DROP TABLE IF EXISTS k_string;
DROP TABLE IF EXISTS k_string_mem;
CREATE TABLE k_string (s String) ENGINE = MergeTree ORDER BY s PARTITION BY s
    SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO k_string VALUES ('01'), ('1');
CREATE TABLE k_string_mem (s String) ENGINE = Memory;
INSERT INTO k_string_mem SELECT * FROM k_string;

DROP TABLE IF EXISTS k_string_length;
CREATE TABLE k_string_length (s String) ENGINE = MergeTree ORDER BY tuple() PARTITION BY length(s)
    SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO k_string_length SELECT * FROM k_string;

DROP TABLE IF EXISTS k_uint8;
DROP TABLE IF EXISTS k_uint8_mem;
CREATE TABLE k_uint8 (k UInt8) ENGINE = MergeTree ORDER BY k PARTITION BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO k_uint8 VALUES (0), (1), (5);
CREATE TABLE k_uint8_mem (k UInt8) ENGINE = Memory;
INSERT INTO k_uint8_mem SELECT * FROM k_uint8;

DROP TABLE IF EXISTS k_variant;
DROP TABLE IF EXISTS k_variant_mem;
CREATE TABLE k_variant (k String) ENGINE = MergeTree ORDER BY k PARTITION BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO k_variant VALUES ('3'), ('4');
CREATE TABLE k_variant_mem (k String) ENGINE = Memory;
INSERT INTO k_variant_mem SELECT * FROM k_variant;

DROP TABLE IF EXISTS k_tuple;
DROP TABLE IF EXISTS k_tuple_mem;
CREATE TABLE k_tuple (a String, b UInt64) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b)
    SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO k_tuple VALUES ('01', 1), ('1', 1);
CREATE TABLE k_tuple_mem (a String, b UInt64) ENGINE = Memory;
INSERT INTO k_tuple_mem SELECT * FROM k_tuple;

DROP TABLE IF EXISTS k_datetime;
DROP TABLE IF EXISTS k_datetime_mem;
CREATE TABLE k_datetime (d DateTime('Europe/Moscow')) ENGINE = MergeTree ORDER BY d PARTITION BY d
    SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO k_datetime VALUES (toDateTime(1675195200, 'Europe/Moscow')), (toDateTime(1675195260, 'Europe/Moscow'));
CREATE TABLE k_datetime_mem (d DateTime('Europe/Moscow')) ENGINE = Memory;
INSERT INTO k_datetime_mem SELECT * FROM k_datetime;

DROP TABLE IF EXISTS k_lowcard;
DROP TABLE IF EXISTS k_lowcard_mem;
CREATE TABLE k_lowcard (s LowCardinality(String)) ENGINE = MergeTree ORDER BY s PARTITION BY s
    SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO k_lowcard VALUES ('a'), ('b');
CREATE TABLE k_lowcard_mem (s LowCardinality(String)) ENGINE = Memory;
INSERT INTO k_lowcard_mem SELECT * FROM k_lowcard;

DROP TABLE IF EXISTS k_tuple_bool;
DROP TABLE IF EXISTS k_tuple_bool_mem;
CREATE TABLE k_tuple_bool (tup Tuple(UInt8)) ENGINE = MergeTree ORDER BY tup PARTITION BY tup
    SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO k_tuple_bool VALUES ((0)), ((1)), ((5));
CREATE TABLE k_tuple_bool_mem (tup Tuple(UInt8)) ENGINE = Memory;
INSERT INTO k_tuple_bool_mem SELECT * FROM k_tuple_bool;

DROP TABLE IF EXISTS k_tuple_lc;
DROP TABLE IF EXISTS k_tuple_lc_mem;
CREATE TABLE k_tuple_lc (tup Tuple(LowCardinality(String))) ENGINE = MergeTree ORDER BY tup PARTITION BY tup
    SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO k_tuple_lc VALUES (('a')), (('b'));
CREATE TABLE k_tuple_lc_mem (tup Tuple(LowCardinality(String))) ENGINE = Memory;
INSERT INTO k_tuple_lc_mem SELECT * FROM k_tuple_lc;

SELECT 'UInt64 key, String element, NOT IN',
       (SELECT groupArray(k) FROM (SELECT k FROM k_uint64 WHERE k NOT IN (SELECT '01') ORDER BY k))
     = (SELECT groupArray(k) FROM (SELECT k FROM k_uint64_mem WHERE k NOT IN (SELECT '01') ORDER BY k));

SELECT 'String key, UInt8 element, IN',
       (SELECT groupArray(s) FROM (SELECT s FROM k_string WHERE s IN (SELECT 1) ORDER BY s))
     = (SELECT groupArray(s) FROM (SELECT s FROM k_string_mem WHERE s IN (SELECT 1) ORDER BY s));

SELECT 'String key transformed by length(), UInt8 element, IN',
       (SELECT groupArray(s) FROM (SELECT s FROM k_string_length WHERE s IN (SELECT 1) ORDER BY s))
     = (SELECT groupArray(s) FROM (SELECT s FROM k_string_mem WHERE s IN (SELECT 1) ORDER BY s));

SELECT 'UInt8 key, Bool element, IN',
       (SELECT groupArray(k) FROM (SELECT k FROM k_uint8 WHERE k IN (SELECT CAST(5, 'Bool')) ORDER BY k))
     = (SELECT groupArray(k) FROM (SELECT k FROM k_uint8_mem WHERE k IN (SELECT CAST(5, 'Bool')) ORDER BY k));

SELECT 'String key, Variant element, NOT IN',
       (SELECT groupArray(k) FROM (SELECT k FROM k_variant WHERE k NOT IN (SELECT CAST(toUInt8(3), 'Variant(String, UInt8)')) ORDER BY k))
     = (SELECT groupArray(k) FROM (SELECT k FROM k_variant_mem WHERE k NOT IN (SELECT CAST(toUInt8(3), 'Variant(String, UInt8)')) ORDER BY k));

SELECT '(String, UInt64) key, Tuple element, IN',
       (SELECT groupArray((a, b)) FROM (SELECT a, b FROM k_tuple WHERE (a, b) IN (SELECT (1, 1)) ORDER BY a))
     = (SELECT groupArray((a, b)) FROM (SELECT a, b FROM k_tuple_mem WHERE (a, b) IN (SELECT (1, 1)) ORDER BY a));

-- Controls: pairs whose conversion does preserve equality must keep pruning exactly. Each asserts the
-- surviving `Parts: 1/2` as well as the answer, so an over-broad rule that declined them would fail here.
SELECT 'control UInt64 key, UInt64 element, prunes', count() > 0
FROM (EXPLAIN indexes = 1 SELECT sum(k) FROM k_uint64 WHERE k IN (SELECT toUInt64(1)))
WHERE explain ILIKE '%Parts: 1/2%';
SELECT 'control UInt64 key, UInt64 element, answer',
       (SELECT groupArray(k) FROM (SELECT k FROM k_uint64 WHERE k IN (SELECT toUInt64(1)) ORDER BY k))
     = (SELECT groupArray(k) FROM (SELECT k FROM k_uint64_mem WHERE k IN (SELECT toUInt64(1)) ORDER BY k));

SELECT 'control UInt64 key, UInt8 element, prunes', count() > 0
FROM (EXPLAIN indexes = 1 SELECT sum(k) FROM k_uint64 WHERE k IN (SELECT toUInt8(1)))
WHERE explain ILIKE '%Parts: 1/2%';
SELECT 'control UInt64 key, UInt8 element, answer',
       (SELECT groupArray(k) FROM (SELECT k FROM k_uint64 WHERE k IN (SELECT toUInt8(1)) ORDER BY k))
     = (SELECT groupArray(k) FROM (SELECT k FROM k_uint64_mem WHERE k IN (SELECT toUInt8(1)) ORDER BY k));

-- A time zone is display metadata that no `Field` carries, so the two `DateTime`s stay interchangeable.
SELECT 'control DateTime key, other-timezone element, prunes', count() > 0
FROM (EXPLAIN indexes = 1 SELECT sum(toUnixTimestamp(d)) FROM k_datetime WHERE d IN (SELECT toDateTime(1675195200, 'UTC')))
WHERE explain ILIKE '%Parts: 1/2%';
SELECT 'control DateTime key, other-timezone element, IN answer',
       (SELECT groupArray(d) FROM (SELECT d FROM k_datetime WHERE d IN (SELECT toDateTime(1675195200, 'UTC')) ORDER BY d))
     = (SELECT groupArray(d) FROM (SELECT d FROM k_datetime_mem WHERE d IN (SELECT toDateTime(1675195200, 'UTC')) ORDER BY d));
SELECT 'control DateTime key, other-timezone element, NOT IN answer',
       (SELECT groupArray(d) FROM (SELECT d FROM k_datetime WHERE d NOT IN (SELECT toDateTime(1675195200, 'UTC')) ORDER BY d))
     = (SELECT groupArray(d) FROM (SELECT d FROM k_datetime_mem WHERE d NOT IN (SELECT toDateTime(1675195200, 'UTC')) ORDER BY d));

-- `LowCardinality` is stripped from nested types too, so an identical composite is not declined.
SELECT 'control LowCardinality(String) key, String element, prunes', count() > 0
FROM (EXPLAIN indexes = 1 SELECT sum(length(s)) FROM k_lowcard WHERE s IN (SELECT 'a'))
WHERE explain ILIKE '%Parts: 1/2%';
SELECT 'control LowCardinality(String) key, String element, answer',
       (SELECT groupArray(s) FROM (SELECT s FROM k_lowcard WHERE s IN (SELECT 'a') ORDER BY s))
     = (SELECT groupArray(s) FROM (SELECT s FROM k_lowcard_mem WHERE s IN (SELECT 'a') ORDER BY s));

-- The two composite cells below reach the type check recursively, where `Tuple`'s own `equals` cannot: it
-- ignores custom names and compares a nested `LowCardinality` as itself. Nested inside a `Tuple`, `Bool`
-- relabels the element without clamping the stored integer the way a top-level `Bool` cast does, so this
-- pair converts losslessly both ways and no row is dropped whether or not the atom is built.
SELECT 'control Tuple(UInt8) key, Tuple(Bool) element, declines',
       (SELECT count() > 0
        FROM (EXPLAIN indexes = 1 SELECT sum(tup.1) FROM k_tuple_bool WHERE tup IN (SELECT CAST(tuple(5), 'Tuple(Bool)')))
        WHERE explain ILIKE '%Parts: 3/3%') AS declines,
       (SELECT groupArray(tup) FROM (SELECT tup FROM k_tuple_bool WHERE tup IN (SELECT CAST(tuple(5), 'Tuple(Bool)')) ORDER BY tup))
     = (SELECT groupArray(tup) FROM (SELECT tup FROM k_tuple_bool_mem WHERE tup IN (SELECT CAST(tuple(5), 'Tuple(Bool)')) ORDER BY tup)) AS answer;

-- An outer-only strip would decline this one, because `LowCardinality(String)` and `String` are not
-- `equals`-equal, so it pins the pruning the recursive strip buys.
SELECT 'control Tuple(LowCardinality(String)) key, Tuple(String) element, prunes',
       (SELECT count() > 0
        FROM (EXPLAIN indexes = 1 SELECT sum(length(tup.1)) FROM k_tuple_lc WHERE tup IN (SELECT CAST(tuple('a'), 'Tuple(String)')))
        WHERE explain ILIKE '%Parts: 1/2%') AS prunes,
       (SELECT groupArray(tup) FROM (SELECT tup FROM k_tuple_lc WHERE tup IN (SELECT CAST(tuple('a'), 'Tuple(String)')) ORDER BY tup))
     = (SELECT groupArray(tup) FROM (SELECT tup FROM k_tuple_lc_mem WHERE tup IN (SELECT CAST(tuple('a'), 'Tuple(String)')) ORDER BY tup)) AS answer;
