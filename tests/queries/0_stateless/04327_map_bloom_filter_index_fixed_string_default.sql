DROP TABLE IF EXISTS t_map_bf_fixed_string;

CREATE TABLE t_map_bf_fixed_string
(
    row_id UInt32,
    map Map(String, String),
    INDEX idx mapKeys(map) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO t_map_bf_fixed_string VALUES (0, {'K0':'V0'}), (1, {'K1':'V1'});

SELECT 'Absent key compared with FixedString default (matches String default at runtime)';
SELECT count() FROM t_map_bf_fixed_string WHERE map[''] = toFixedString('', 3);
SELECT count() FROM t_map_bf_fixed_string WHERE map[''] = toFixedString('', 3) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_map_bf_fixed_string WHERE map[''] = toFixedString('', 3) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_map_bf_fixed_string WHERE map[''] = '';

SELECT 'Present key with FixedString constant still uses the index';
SELECT count() FROM t_map_bf_fixed_string WHERE map['K0'] = toFixedString('V0', 2) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_bf_fixed_string WHERE map['K0'] = toFixedString('V0', 2))
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));

SELECT 'Absent key with non-default FixedString constant is still pruned';
SELECT count() FROM t_map_bf_fixed_string WHERE map['K2'] = toFixedString('V2', 2) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_bf_fixed_string WHERE map['K2'] = toFixedString('V2', 2))
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));

DROP TABLE t_map_bf_fixed_string;

DROP TABLE IF EXISTS t_map_bf_int_default;

CREATE TABLE t_map_bf_int_default
(
    row_id UInt32,
    map Map(String, UInt64),
    INDEX idx mapKeys(map) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO t_map_bf_int_default VALUES (0, {'K0':10}), (1, {'K1':20});

SELECT 'Absent key compared with integer default 0 via different integer type';
SELECT count() FROM t_map_bf_int_default WHERE map['K2'] = toInt8(0);
SELECT count() FROM t_map_bf_int_default WHERE map['K2'] = toInt8(0) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_map_bf_int_default WHERE map['K2'] = toInt8(0) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT 'Absent key compared with non-default integer is still pruned';
SELECT count() FROM t_map_bf_int_default WHERE map['K2'] = toInt8(5) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_bf_int_default WHERE map['K2'] = toInt8(5))
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));

DROP TABLE t_map_bf_int_default;

DROP TABLE IF EXISTS t_map_values_bf;

CREATE TABLE t_map_values_bf
(
    row_id UInt32,
    map Map(String, String),
    INDEX idx mapValues(map) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO t_map_values_bf VALUES (0, {'K0':'V0'}), (1, {'K1':'V1'});

SELECT 'mapValues: present key matched by FixedString constant is not over-pruned';
SELECT count() FROM t_map_values_bf WHERE map['K0'] = toFixedString('V0', 3);
SELECT count() FROM t_map_values_bf WHERE map['K0'] = toFixedString('V0', 3) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_map_values_bf WHERE map['K0'] = toFixedString('V0', 3) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT 'mapValues: String constant still uses the index';
SELECT count() FROM t_map_values_bf WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_values_bf WHERE map['K0'] = 'V0')
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));

SELECT 'mapValues: present key with non-matching FixedString constant returns nothing';
SELECT count() FROM t_map_values_bf WHERE map['K0'] = toFixedString('VX', 3);
SELECT count() FROM t_map_values_bf WHERE map['K0'] = toFixedString('VX', 3) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE t_map_values_bf;

DROP TABLE IF EXISTS t_map_values_bf_dup;

CREATE TABLE t_map_values_bf_dup
(
    row_id UInt32,
    map Map(String, String),
    INDEX idx mapValues(map) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY row_id SETTINGS index_granularity = 1;

-- 'V0' and 'V0\0' are distinct stored strings (different bloom hashes) but both equal
-- toFixedString('V0', 3) at runtime, so no single hash can represent the match: the index
-- must be skipped here, otherwise one of the matching granules would be wrongly pruned.
INSERT INTO t_map_values_bf_dup VALUES (0, {'K':'V0'}), (1, {'K':'V0\0'});

SELECT 'mapValues: two distinct stored values both matching one FixedString constant are both kept';
SELECT count() FROM t_map_values_bf_dup WHERE map['K'] = toFixedString('V0', 3);
SELECT count() FROM t_map_values_bf_dup WHERE map['K'] = toFixedString('V0', 3) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_map_values_bf_dup WHERE map['K'] = toFixedString('V0', 3) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE t_map_values_bf_dup;

DROP TABLE IF EXISTS t_map_values_bf_fs;

CREATE TABLE t_map_values_bf_fs
(
    row_id UInt32,
    map Map(String, FixedString(3)),
    INDEX idx mapValues(map) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO t_map_values_bf_fs VALUES (0, {'K0':'V0'}), (1, {'K1':'W0'});

SELECT 'mapValues over FixedString index: same-width FixedString constant still uses the index';
SELECT count() FROM t_map_values_bf_fs WHERE map['K0'] = toFixedString('V0', 3) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_values_bf_fs WHERE map['K0'] = toFixedString('V0', 3))
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));

SELECT 'mapValues over FixedString index: different-width FixedString constant is not over-pruned';
SELECT count() FROM t_map_values_bf_fs WHERE map['K0'] = toFixedString('V0', 5);
SELECT count() FROM t_map_values_bf_fs WHERE map['K0'] = toFixedString('V0', 5) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_map_values_bf_fs WHERE map['K0'] = toFixedString('V0', 5) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE t_map_values_bf_fs;

DROP TABLE IF EXISTS t_map_values_bf_num;

CREATE TABLE t_map_values_bf_num
(
    row_id UInt32,
    map Map(String, UInt64),
    INDEX idx mapValues(map) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO t_map_values_bf_num VALUES (0, {'K':1}), (1, {'K':2}), (2, {'K':3});

-- The comparison constant keeps its own type, so it has to be converted to the indexed value type
-- before it can be hashed. Without the conversion these predicates fail with BAD_GET instead of
-- either using or skipping the index. `prunes` asserts fewer granules were read than exist, which
-- a silent fallback to a full scan would not satisfy.
SELECT 'mapValues over UInt64 index: exactly representable Float64 constant prunes';
SELECT count() FROM t_map_values_bf_num WHERE map['K'] = 1.0;
SELECT count() FROM t_map_values_bf_num WHERE map['K'] = 1.0 SETTINGS use_skip_indexes = 0;
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_values_bf_num WHERE map['K'] = 1.0)
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));

SELECT 'mapValues over UInt64 index: Int128 constant prunes';
SELECT count() FROM t_map_values_bf_num WHERE map['K'] = toInt128(2);
SELECT count() FROM t_map_values_bf_num WHERE map['K'] = toInt128(2) SETTINGS use_skip_indexes = 0;
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_values_bf_num WHERE map['K'] = toInt128(2))
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));

-- A Decimal constant against an integer index is not convertible, so the index is declined. The
-- answer must still be correct, which is what previously failed with BAD_GET.
SELECT 'mapValues over UInt64 index: Decimal constant declines the index';
SELECT count() FROM t_map_values_bf_num WHERE map['K'] = toDecimal64(2, 0);
SELECT count() FROM t_map_values_bf_num WHERE map['K'] = toDecimal64(2, 0) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_map_values_bf_num WHERE map['K'] = toDecimal64(2, 0) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- A constant with no exact representation in the indexed type cannot be described by a single hash,
-- so the index is declined rather than used with a rounded value. Asserting INDEX_NOT_USED pins the
-- fallback itself, which matching counts alone would not distinguish from using the index.
SELECT 'mapValues over UInt64 index: constant with no exact representation declines the index';
SELECT count() FROM t_map_values_bf_num WHERE map['K'] = 1.5;
SELECT count() FROM t_map_values_bf_num WHERE map['K'] = 1.5 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_map_values_bf_num WHERE map['K'] = 1.5 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT 'mapValues over UInt64 index: heterogeneous integer constants still use the index';
SELECT count() FROM t_map_values_bf_num WHERE map['K'] = toInt64(2) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_values_bf_num WHERE map['K'] = toInt64(2))
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));
SELECT count() FROM t_map_values_bf_num WHERE map['K'] = toUInt8(2) SETTINGS force_data_skipping_indices = 'idx';
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_values_bf_num WHERE map['K'] = toUInt8(2))
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));

DROP TABLE t_map_values_bf_num;

DROP TABLE IF EXISTS t_map_values_bf_enum;

CREATE TABLE t_map_values_bf_enum
(
    row_id UInt32,
    map Map(String, Enum8('one' = 1, 'two' = 2)),
    INDEX idx mapValues(map) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO t_map_values_bf_enum VALUES (0, {'K':'one'}), (1, {'K':'two'});

SELECT 'mapValues over Enum8 index: String constant prunes';
SELECT count() FROM t_map_values_bf_enum WHERE map['K'] = 'two';
SELECT count() FROM t_map_values_bf_enum WHERE map['K'] = 'two' SETTINGS use_skip_indexes = 0;
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_values_bf_enum WHERE map['K'] = 'two')
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));

-- A numeric constant that is not a declared member of the enum is simply never equal. The index has
-- to decline instead of surfacing the conversion failure as a query error.
SELECT 'mapValues over Enum8 index: numeric constant outside the enum declines the index';
SELECT count() FROM t_map_values_bf_enum WHERE map['K'] = toInt8(3);
SELECT count() FROM t_map_values_bf_enum WHERE map['K'] = toInt8(3) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_map_values_bf_enum WHERE map['K'] = toInt8(3) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- A wider integer constant outside the enum range reaches a different conversion failure, and must
-- decline the index the same way.
SELECT 'mapValues over Enum8 index: out-of-range wider integer constant declines the index';
SELECT count() FROM t_map_values_bf_enum WHERE map['K'] = toInt16(300);
SELECT count() FROM t_map_values_bf_enum WHERE map['K'] = toInt16(300) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_map_values_bf_enum WHERE map['K'] = toInt16(300) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE t_map_values_bf_enum;

DROP TABLE IF EXISTS t_map_values_bf_date;

CREATE TABLE t_map_values_bf_date
(
    row_id UInt32,
    map Map(String, Date),
    INDEX idx mapValues(map) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO t_map_values_bf_date VALUES (0, {'K':'2020-01-01'}), (1, {'K':'2021-01-01'});

SELECT 'mapValues over Date index: String constant prunes';
SELECT count() FROM t_map_values_bf_date WHERE map['K'] = '2021-01-01';
SELECT count() FROM t_map_values_bf_date WHERE map['K'] = '2021-01-01' SETTINGS use_skip_indexes = 0;
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_values_bf_date WHERE map['K'] = '2021-01-01')
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));

DROP TABLE t_map_values_bf_date;

DROP TABLE IF EXISTS t_map_values_bf_lc;

CREATE TABLE t_map_values_bf_lc
(
    row_id UInt32,
    map Map(String, String),
    INDEX idx mapValues(map) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO t_map_values_bf_lc VALUES (0, {'K':'V0'}), (1, {'K':'V1'}), (2, {'K':'V2'});

-- The index stays usable exactly when the constant's primitive type equals the indexed one, so a
-- String constant keeps it whatever produced the String. `toLowCardinality` is folded to a plain
-- String before index analysis, so it is covered here rather than by the wrapper cases below.
SELECT 'mapValues over String index: String constant of a different origin still uses the index';
SELECT count() FROM t_map_values_bf_lc WHERE map['K'] = toLowCardinality('V1') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_values_bf_lc WHERE map['K'] = toLowCardinality('V1'))
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));
SELECT count() FROM t_map_values_bf_lc WHERE map['K'] = toLowCardinality('V1') SETTINGS use_skip_indexes = 0;

DROP TABLE t_map_values_bf_lc;

DROP TABLE IF EXISTS t_map_values_bf_enum_const;

CREATE TABLE t_map_values_bf_enum_const
(
    row_id UInt32,
    map Map(String, String),
    INDEX idx mapValues(map) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO t_map_values_bf_enum_const VALUES (0, {'K':'V0'}), (1, {'K':'V1'});

-- The index is usable only for a constant that stores the indexed representation. An Enum constant
-- stores its numeric value, which converts to the name's digits rather than to the name itself while
-- equality compares against the name, so it does not, and the index has to be declined.
SELECT 'mapValues over String index: Enum constant declines the index';
SELECT count() FROM t_map_values_bf_enum_const WHERE map['K'] = CAST('V0', 'Enum8(\'V0\' = 1, \'V1\' = 2)');
SELECT count() FROM t_map_values_bf_enum_const WHERE map['K'] = CAST('V0', 'Enum8(\'V0\' = 1, \'V1\' = 2)') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_map_values_bf_enum_const WHERE map['K'] = CAST('V0', 'Enum8(\'V0\' = 1, \'V1\' = 2)') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE t_map_values_bf_enum_const;

DROP TABLE IF EXISTS t_map_values_bf_wrapped;

CREATE TABLE t_map_values_bf_wrapped
(
    row_id UInt32,
    map Map(String, String),
    INDEX idx mapValues(map) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO t_map_values_bf_wrapped VALUES (0, {'K':'V0'}), (1, {'K':'V1'});

-- A wrapper does not change what the constant stores, so the decision has to look through it: a
-- LowCardinality(String) constant is still a String and keeps the index, while a
-- LowCardinality(FixedString) or a Variant holding a FixedString is a different representation and
-- must decline it. optimize_functions_to_subcolumns picks between the two map access rewrites and is
-- randomized by the test runner, so both values are pinned and exercised here.
SELECT 'mapValues over String index: LowCardinality(FixedString) constant declines the index, subcolumns=1';
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = toLowCardinality(toFixedString('V0', 3)) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = toLowCardinality(toFixedString('V0', 3)) SETTINGS use_skip_indexes = 0, optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = toLowCardinality(toFixedString('V0', 3)) SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 1; -- { serverError INDEX_NOT_USED }

SELECT 'mapValues over String index: Variant constant holding a FixedString declines the index, subcolumns=1';
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = CAST(toFixedString('V0', 3), 'Variant(FixedString(3), UInt64)') SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = CAST(toFixedString('V0', 3), 'Variant(FixedString(3), UInt64)') SETTINGS use_skip_indexes = 0, optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = CAST(toFixedString('V0', 3), 'Variant(FixedString(3), UInt64)') SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 1; -- { serverError INDEX_NOT_USED }

SELECT 'mapValues over String index: LowCardinality(String) constant keeps the index, subcolumns=1';
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = toLowCardinality('V1') SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 1;
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = toLowCardinality('V1') SETTINGS optimize_functions_to_subcolumns = 1)
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = toLowCardinality('V1') SETTINGS use_skip_indexes = 0, optimize_functions_to_subcolumns = 1;

-- A wrapper does not change what the constant stores, so the decision has to look through it: a
-- LowCardinality(String) constant is still a String and keeps the index, while a
-- LowCardinality(FixedString) or a Variant holding a FixedString is a different representation and
-- must decline it. optimize_functions_to_subcolumns picks between the two map access rewrites and is
-- randomized by the test runner, so both values are pinned and exercised here.
SELECT 'mapValues over String index: LowCardinality(FixedString) constant declines the index, subcolumns=0';
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = toLowCardinality(toFixedString('V0', 3)) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = toLowCardinality(toFixedString('V0', 3)) SETTINGS use_skip_indexes = 0, optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = toLowCardinality(toFixedString('V0', 3)) SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 0; -- { serverError INDEX_NOT_USED }

SELECT 'mapValues over String index: Variant constant holding a FixedString declines the index, subcolumns=0';
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = CAST(toFixedString('V0', 3), 'Variant(FixedString(3), UInt64)') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = CAST(toFixedString('V0', 3), 'Variant(FixedString(3), UInt64)') SETTINGS use_skip_indexes = 0, optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = CAST(toFixedString('V0', 3), 'Variant(FixedString(3), UInt64)') SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 0; -- { serverError INDEX_NOT_USED }

SELECT 'mapValues over String index: LowCardinality(String) constant keeps the index, subcolumns=0';
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = toLowCardinality('V1') SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 0;
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = toLowCardinality('V1') SETTINGS optimize_functions_to_subcolumns = 0)
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));
SELECT count() FROM t_map_values_bf_wrapped WHERE map['K'] = toLowCardinality('V1') SETTINGS use_skip_indexes = 0, optimize_functions_to_subcolumns = 0;

DROP TABLE t_map_values_bf_wrapped;

DROP TABLE IF EXISTS t_map_keys_bf_nullable;

CREATE TABLE t_map_keys_bf_nullable
(
    row_id UInt32,
    map Map(String, Nullable(String)),
    INDEX idx mapKeys(map) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO t_map_keys_bf_nullable VALUES (0, {'K0':'V0'}), (1, {'K1':'V1'}), (2, {'K2':'V2'});

-- A Nullable value type has a NULL default, and `NULL = const` is NULL rather than true, so an absent
-- key matches neither `equals` nor `notEquals` and the granule can still be pruned.
SELECT 'mapKeys over Nullable value type: absent key cannot match, index stays usable';
SELECT count() FROM t_map_keys_bf_nullable WHERE map['K1'] = 'V1' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() > 0 AS prunes FROM (EXPLAIN indexes = 1 SELECT count() FROM t_map_keys_bf_nullable WHERE map['K1'] = 'V1')
    WHERE explain LIKE '%Granules: %/%'
      AND toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'));
SELECT count() FROM t_map_keys_bf_nullable WHERE map['K1'] = 'V1' SETTINGS use_skip_indexes = 0;

DROP TABLE t_map_keys_bf_nullable;
