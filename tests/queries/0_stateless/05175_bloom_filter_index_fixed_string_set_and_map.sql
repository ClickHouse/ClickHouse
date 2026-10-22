-- https://github.com/ClickHouse/ClickHouse/issues/116407
-- The `bloom_filter` index hashes the exact bytes of a constant, while the comparison it stands in
-- for is padding-aware for the string family. Two paths still hashed the wrong byte length and pruned
-- every granule: a set coming from a subquery or a table (which keeps its own element type, unlike a
-- literal `IN` list), and `m['k'] = const` over a `mapValues` index (hashed without coercing the
-- constant to the map's value type).

DROP TABLE IF EXISTS t_bf_set;
CREATE TABLE t_bf_set (v String, INDEX bf v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_set VALUES ('V0'), ('V0\0'), ('V0\0\0'), ('other1'), ('other2');
OPTIMIZE TABLE t_bf_set FINAL;

SELECT 'ground truth';
SELECT 'V0\0' = 'V0'::FixedString(3);

SELECT 'subquery set';
SELECT count() FROM t_bf_set WHERE v IN (SELECT 'V0'::FixedString(3));
SELECT count() FROM t_bf_set WHERE v IN (SELECT 'V0'::FixedString(3)) SETTINGS use_skip_indexes = 0;

SELECT 'literal set';
SELECT count() FROM t_bf_set WHERE v IN ('V0'::FixedString(3));
SELECT count() FROM t_bf_set WHERE v IN ('V0'::FixedString(3)) SETTINGS use_skip_indexes = 0;

SELECT 'same-type subquery set still prunes';
SELECT count() FROM t_bf_set WHERE v IN (SELECT 'V0');
SELECT count() FROM t_bf_set WHERE v IN (SELECT 'V0') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_set WHERE v IN (SELECT 'nosuch');
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_set WHERE v IN (SELECT 'nosuch')) WHERE explain LIKE '%Granules: 0/%';

SELECT 'map value';
DROP TABLE IF EXISTS t_bf_map;
CREATE TABLE t_bf_map (m Map(String, FixedString(3)), INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map VALUES (map('k', 'V0')), (map('k', 'ab')), (map('k', 'xyz'));
OPTIMIZE TABLE t_bf_map FINAL;

SELECT count() FROM t_bf_map WHERE m['k'] = 'V0';
SELECT count() FROM t_bf_map WHERE m['k'] = 'V0' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map WHERE m['k'] = 'xyz';
SELECT count() FROM t_bf_map WHERE m['k'] = 'xyz' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map WHERE m['k'] = 'nos';
SELECT count() FROM t_bf_map WHERE m['k'] = 'nos' SETTINGS use_skip_indexes = 0;

SELECT 'map value from a subquery set';
-- The `mapValues` arm of the set path casts the set column to the map's value type the same way, so a
-- `FixedString` element over a `String` value type hashes the stripped bytes and prunes every granule.
DROP TABLE IF EXISTS t_bf_map_str;
CREATE TABLE t_bf_map_str (m Map(String, String), INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_str VALUES (map('k', 'V0')), (map('k', 'V0\0')), (map('k', 'xx'));
OPTIMIZE TABLE t_bf_map_str FINAL;

SELECT count() FROM t_bf_map_str WHERE m['k'] IN (SELECT 'V0'::FixedString(3));
SELECT count() FROM t_bf_map_str WHERE m['k'] IN (SELECT 'V0'::FixedString(3)) SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_bf_map_set_source;
CREATE TABLE t_bf_map_set_source (v FixedString(3)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_bf_map_set_source VALUES ('V0');

SELECT count() FROM t_bf_map_str WHERE m['k'] IN (SELECT v FROM t_bf_map_set_source);
SELECT count() FROM t_bf_map_str WHERE m['k'] IN (SELECT v FROM t_bf_map_set_source) SETTINGS use_skip_indexes = 0;

SELECT 'same-type subquery set over a map still prunes';
SELECT count() FROM t_bf_map_str WHERE m['k'] IN (SELECT 'nosuch');
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_map_str WHERE m['k'] IN (SELECT 'nosuch')) WHERE explain LIKE '%Granules: 0/%';

SELECT 'absent map key with a FixedString value';
-- `arrayElement` returns the value type's materialized default for a missing key, and for
-- `FixedString(3)` that default is three zero bytes rather than the empty `Field`. The index has to
-- decline for such a constant, otherwise both map arms prune the granules where the key is absent.
DROP TABLE IF EXISTS t_bf_map_absent_values;
CREATE TABLE t_bf_map_absent_values (m Map(String, FixedString(3)), INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_absent_values VALUES (map('k', 'abc')), (map('other', 'xyz')), (map());
OPTIMIZE TABLE t_bf_map_absent_values FINAL;

SELECT count() FROM t_bf_map_absent_values WHERE m['absent'] = toFixedString('', 3);
SELECT count() FROM t_bf_map_absent_values WHERE m['absent'] = toFixedString('', 3) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map_absent_values WHERE m['absent'] = '';
SELECT count() FROM t_bf_map_absent_values WHERE m['absent'] = '' SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_bf_map_absent_keys;
CREATE TABLE t_bf_map_absent_keys (m Map(String, FixedString(3)), INDEX bf mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_absent_keys VALUES (map('k', 'abc')), (map('other', 'xyz')), (map());
OPTIMIZE TABLE t_bf_map_absent_keys FINAL;

SELECT count() FROM t_bf_map_absent_keys WHERE m['absent'] = toFixedString('', 3);
SELECT count() FROM t_bf_map_absent_keys WHERE m['absent'] = toFixedString('', 3) SETTINGS use_skip_indexes = 0;

SELECT 'absent map key with an all-zero String constant';
-- The comparison is padding-aware: a `String` constant of any length that holds only zero bytes is
-- equal to the `FixedString(3)` default of a missing key, although it is no default of `String`.
-- The guard has to decide the question in the map's value type, or both arms prune those rows.
SELECT count() FROM t_bf_map_absent_values WHERE m['absent'] = '\0';
SELECT count() FROM t_bf_map_absent_values WHERE m['absent'] = '\0' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map_absent_values WHERE m['absent'] = '\0\0\0\0';
SELECT count() FROM t_bf_map_absent_values WHERE m['absent'] = '\0\0\0\0' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map_absent_keys WHERE m['absent'] = '\0';
SELECT count() FROM t_bf_map_absent_keys WHERE m['absent'] = '\0' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map_absent_keys WHERE m['absent'] = '\0\0\0\0';
SELECT count() FROM t_bf_map_absent_keys WHERE m['absent'] = '\0\0\0\0' SETTINGS use_skip_indexes = 0;

SELECT 'a String value type compares exactly, so a zero byte still prunes';
-- Over `Map(String, String)` the missing key is the empty string and `equals` of two `String`
-- values is exact, so `'\0'` cannot match a missing key and the index may keep pruning.
SELECT count() FROM t_bf_map_str WHERE m['absent'] = '\0';
SELECT count() FROM t_bf_map_str WHERE m['absent'] = '\0' SETTINGS use_skip_indexes = 0;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_map_str WHERE m['absent'] = '\0') WHERE explain LIKE '%Granules: 0/%';

SELECT 'a non-default FixedString constant still prunes';
SELECT count() FROM t_bf_map_absent_keys WHERE m['absent'] = toFixedString('abc', 3);
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_map_absent_keys WHERE m['absent'] = toFixedString('abc', 3)) WHERE explain LIKE '%Granules: 0/%';

SELECT 'absent map key against a set of another type';
-- `Set::execute` casts the missing key's `UInt8` default `0` to the set's `String` type, where it
-- matches `'0'`, so the guard has to probe the set with the default of the map value type.
DROP TABLE IF EXISTS t_bf_map_uint8_keys;
CREATE TABLE t_bf_map_uint8_keys (m Map(String, UInt8), INDEX bf mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_uint8_keys VALUES (map('k', 1)), (map('other', 2)), (map());
OPTIMIZE TABLE t_bf_map_uint8_keys FINAL;

DROP TABLE IF EXISTS t_bf_map_uint8_set_source;
CREATE TABLE t_bf_map_uint8_set_source (s String) ENGINE = Memory;
INSERT INTO t_bf_map_uint8_set_source VALUES ('0');

SELECT count() FROM t_bf_map_uint8_keys WHERE m['absent'] IN (SELECT '0');
SELECT count() FROM t_bf_map_uint8_keys WHERE m['absent'] IN (SELECT '0') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map_uint8_keys WHERE m['absent'] IN (SELECT s FROM t_bf_map_uint8_set_source);
SELECT count() FROM t_bf_map_uint8_keys WHERE m['absent'] IN (SELECT s FROM t_bf_map_uint8_set_source) SETTINGS use_skip_indexes = 0;
-- A set without the default still prunes.
SELECT count() FROM t_bf_map_uint8_keys WHERE m['absent'] IN (SELECT '5');
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_map_uint8_keys WHERE m['absent'] IN (SELECT '5')) WHERE explain LIKE '%Granules: 0/%';

DROP TABLE t_bf_map_uint8_keys;

SELECT 'unknown enum label against a mapValues index';
-- With `validate_enum_literals_in_operators = 0` the comparison is constant false, and the index
-- must not throw while converting the label to the `Enum8` value type.
DROP TABLE IF EXISTS t_bf_map_enum;
CREATE TABLE t_bf_map_enum (m Map(String, Enum8('a' = 1, 'b' = 2)), INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_enum VALUES (map('k', 'a')), (map('k', 'b')), (map());
OPTIMIZE TABLE t_bf_map_enum FINAL;

SELECT count() FROM t_bf_map_enum WHERE m['k'] = 'no_such_label' SETTINGS validate_enum_literals_in_operators = 0;
SELECT count() FROM t_bf_map_enum WHERE m['k'] = 'no_such_label' SETTINGS validate_enum_literals_in_operators = 0, use_skip_indexes = 0;
SELECT count() FROM t_bf_map_enum WHERE m['k'] = 'b';
SELECT count() FROM t_bf_map_enum WHERE m['k'] = 'no_such_label' SETTINGS validate_enum_literals_in_operators = 1; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

DROP TABLE t_bf_map_enum;

SELECT 'nullable subquery or table set against a mapValues index';
-- A NULL in the set matches no value of the non-`Nullable` value type; the index must not throw
-- while casting the set to that type.
DROP TABLE IF EXISTS t_bf_map_null_set;
CREATE TABLE t_bf_map_null_set (m Map(String, String), INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_null_set VALUES (map('k', 'a')), (map('k', 'b')), (map());
OPTIMIZE TABLE t_bf_map_null_set FINAL;

DROP TABLE IF EXISTS t_bf_map_null_set_source;
CREATE TABLE t_bf_map_null_set_source (s Nullable(String)) ENGINE = Memory;
INSERT INTO t_bf_map_null_set_source VALUES (NULL), ('b');

SELECT count() FROM t_bf_map_null_set WHERE m['k'] IN (SELECT CAST(NULL AS Nullable(String)));
SELECT count() FROM t_bf_map_null_set WHERE m['k'] IN (SELECT s FROM t_bf_map_null_set_source);
SELECT count() FROM t_bf_map_null_set WHERE m['k'] IN (SELECT s FROM t_bf_map_null_set_source) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map_null_set WHERE toNullable(m['k']) IN (SELECT s FROM t_bf_map_null_set_source);
SELECT count() FROM t_bf_map_null_set WHERE toNullable(m['k']) IN (SELECT s FROM t_bf_map_null_set_source) SETTINGS transform_null_in = 1;
SELECT count() FROM t_bf_map_null_set WHERE m['k'] IN t_bf_map_null_set_source;

DROP TABLE t_bf_map_null_set_source;
DROP TABLE t_bf_map_null_set;

SELECT 'absent map key with a Variant or Dynamic FixedString constant';
-- The active `FixedString` alternative of an erased constant is not visible in its `Field`, so the
-- missing-key guard must decline instead of comparing the default against a rebuilt `String`.
DROP TABLE IF EXISTS t_bf_map_absent_erased;
CREATE TABLE t_bf_map_absent_erased (m Map(String, String), INDEX bf mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_absent_erased VALUES (map('k', 'a')), (map('other', 'b')), (map());
OPTIMIZE TABLE t_bf_map_absent_erased FINAL;

SELECT count() FROM t_bf_map_absent_erased WHERE m['absent'] = CAST(toFixedString('', 1) AS Dynamic);
SELECT count() FROM t_bf_map_absent_erased WHERE m['absent'] = CAST(toFixedString('', 1) AS Dynamic) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map_absent_erased WHERE m['absent'] = CAST(toFixedString('', 1) AS Variant(FixedString(1), UInt8));
SELECT count() FROM t_bf_map_absent_erased WHERE m['absent'] = CAST(toFixedString('', 1) AS Variant(FixedString(1), UInt8)) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_map_absent_erased;

SELECT 'absent map key over Date32 values';
-- A missing `Date32` value is the column default (`1970-01-01`), not `getDefault` (`1900-01-01`).
DROP TABLE IF EXISTS t_bf_map_absent_date32;
CREATE TABLE t_bf_map_absent_date32 (m Map(String, Date32), INDEX bf mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_absent_date32 VALUES (map('k', toDate32('2026-01-01'))), (map('other', toDate32('2026-01-02'))), (map());
OPTIMIZE TABLE t_bf_map_absent_date32 FINAL;

SELECT count() FROM t_bf_map_absent_date32 WHERE m['absent'] = toDate32('1970-01-01');
SELECT count() FROM t_bf_map_absent_date32 WHERE m['absent'] = toDate32('1970-01-01') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map_absent_date32 WHERE m['absent'] IN (SELECT toDate32('1970-01-01'));
SELECT count() FROM t_bf_map_absent_date32 WHERE m['absent'] IN (SELECT toDate32('1970-01-01')) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map_absent_date32 WHERE m['k'] = toDate32('2026-01-01');
SELECT count() FROM t_bf_map_absent_date32 WHERE m['k'] = CAST(toDate32('2026-01-01') AS Dynamic);
SELECT count() FROM t_bf_map_absent_date32 WHERE m['k'] = CAST(toDate32('2026-01-01') AS Dynamic) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_map_absent_date32;

SELECT 'absent map key with a Dynamic Tuple(FixedString) constant';
DROP TABLE IF EXISTS t_bf_map_absent_tuple;
CREATE TABLE t_bf_map_absent_tuple (m Map(String, Tuple(String)), INDEX bf mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_absent_tuple VALUES (map('k', tuple('a'))), (map('other', tuple('b'))), (map());
OPTIMIZE TABLE t_bf_map_absent_tuple FINAL;

SELECT count() FROM t_bf_map_absent_tuple WHERE m['absent'] = CAST(tuple(toFixedString('', 1)) AS Dynamic);
SELECT count() FROM t_bf_map_absent_tuple WHERE m['absent'] = CAST(tuple(toFixedString('', 1)) AS Dynamic) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map_absent_tuple WHERE m['absent'] = tuple(CAST(toFixedString('', 1) AS Dynamic));
SELECT count() FROM t_bf_map_absent_tuple WHERE m['absent'] = tuple(CAST(toFixedString('', 1) AS Dynamic)) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_map_absent_tuple;

SELECT 'map key over Tuple(Enum8) values';
-- The column default of `Tuple(Enum8)` holds the enum zero, which is no valid value and cannot be probed.
DROP TABLE IF EXISTS t_bf_map_tuple_enum;
CREATE TABLE t_bf_map_tuple_enum (m Map(String, Tuple(Enum8('a' = 1, 'b' = 2))), INDEX bf mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_tuple_enum VALUES (map('k', tuple('a'))), (map('k', tuple('b')));
OPTIMIZE TABLE t_bf_map_tuple_enum FINAL;

SELECT count() FROM t_bf_map_tuple_enum WHERE m['k'] IN (SELECT tuple('b'));
SELECT count() FROM t_bf_map_tuple_enum WHERE m['k'] IN (SELECT tuple('b')) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map_tuple_enum WHERE m['k'] = tuple('b');
SELECT count() FROM t_bf_map_tuple_enum WHERE m['k'] = tuple('b') SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_map_tuple_enum;

SELECT 'map key over nested Array values';
-- Only the map keys are indexed, so the map values may be a type the index does not support.
DROP TABLE IF EXISTS t_bf_map_nested_array;
CREATE TABLE t_bf_map_nested_array (m Map(String, Array(Array(UInt8))), INDEX bf mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_nested_array VALUES (map('k', [[1]]));

SELECT count() FROM t_bf_map_nested_array WHERE m['k'] = [[1]];
SELECT count() FROM t_bf_map_nested_array WHERE m['k'] = [[1]] SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_map_nested_array;

SELECT 'map key over Tuple values with a set of another Tuple type';
-- The default of the map value type, `('')`, does not convert to `Tuple(UInt8)` or `Tuple(Date)`,
-- while the stored values do, so the index must not probe the set with it.
DROP TABLE IF EXISTS t_bf_map_tuple_cast;
CREATE TABLE t_bf_map_tuple_cast (m Map(String, Tuple(String)), INDEX bf mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_tuple_cast VALUES (map('k', tuple('1'), 'd', tuple('2020-01-01')));

SELECT count() FROM t_bf_map_tuple_cast WHERE m['k'] IN (SELECT tuple(toUInt8(1)));
SELECT count() FROM t_bf_map_tuple_cast WHERE m['k'] IN (SELECT tuple(toUInt8(1))) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map_tuple_cast WHERE m['d'] IN (SELECT tuple(toDate('2020-01-01')));
SELECT count() FROM t_bf_map_tuple_cast WHERE m['d'] IN (SELECT tuple(toDate('2020-01-01'))) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_map_tuple_cast;
DROP TABLE t_bf_map_uint8_set_source;
DROP TABLE t_bf_set;
DROP TABLE t_bf_map;
DROP TABLE t_bf_map_str;
DROP TABLE t_bf_map_set_source;
DROP TABLE t_bf_map_absent_values;
DROP TABLE t_bf_map_absent_keys;
