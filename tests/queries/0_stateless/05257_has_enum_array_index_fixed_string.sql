-- A `String` or `FixedString` constant searched in an indexed `Array(Enum)` (or in the keys of an indexed
-- `Map` with `Enum` keys) is compared by the name of the enum value, after the cast to the common type
-- `String`, which keeps the zero padding of a `FixedString`. The `bloom_filter` index analysis has to do
-- the same instead of throwing `UNKNOWN_ELEMENT_OF_ENUM`, and a name that is not in the `Enum` matches nothing.

DROP TABLE IF EXISTS t_enum_arr_bf;

CREATE TABLE t_enum_arr_bf
(
    id UInt64,
    arr Array(Enum8('a' = 1, 'b' = 2, 'c' = 3)),
    m Map(Enum8('a' = 1, 'b' = 2, 'c' = 3), UInt64),
    INDEX idx_arr arr TYPE bloom_filter GRANULARITY 1,
    INDEX idx_m mapKeys(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_enum_arr_bf VALUES (1, ['a'], {'a': 1}), (2, ['b'], {'b': 2}), (3, ['c'], {'c': 3});

SELECT 'FixedString';
SELECT id FROM t_enum_arr_bf WHERE has(arr, toFixedString('a', 1)) ORDER BY id;
SELECT id FROM t_enum_arr_bf WHERE indexOf(arr, toFixedString('b', 1)) != 0 ORDER BY id;
SELECT id FROM t_enum_arr_bf WHERE mapContains(m, toFixedString('c', 1)) ORDER BY id;
SELECT id FROM t_enum_arr_bf WHERE has(arr, toFixedString('a', 1)) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM t_enum_arr_bf WHERE mapContains(m, toFixedString('c', 1)) ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT 'padded FixedString';
SELECT id FROM t_enum_arr_bf WHERE has(arr, toFixedString('a', 2)) ORDER BY id;
SELECT id FROM t_enum_arr_bf WHERE indexOf(arr, toFixedString('b', 3)) != 0 ORDER BY id;
SELECT id FROM t_enum_arr_bf WHERE mapContains(m, toFixedString('c', 2)) ORDER BY id;
SELECT id FROM t_enum_arr_bf WHERE has(arr, toFixedString('a', 2)) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM t_enum_arr_bf WHERE mapContains(m, toFixedString('c', 2)) ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT 'Not in the Enum';
SELECT id FROM t_enum_arr_bf WHERE has(arr, 'z') ORDER BY id;
SELECT id FROM t_enum_arr_bf WHERE has(arr, toFixedString('z', 2)) ORDER BY id;
SELECT id FROM t_enum_arr_bf WHERE mapContains(m, 'z') ORDER BY id;

-- The index is used for a name in the `Enum`.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_enum_arr_bf WHERE has(arr, toFixedString('a', 1))) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

DROP TABLE t_enum_arr_bf;
