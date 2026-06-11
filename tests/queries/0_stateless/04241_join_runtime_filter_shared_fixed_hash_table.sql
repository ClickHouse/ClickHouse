-- Tags: no-random-settings

DROP TABLE IF EXISTS phjf_build;
DROP TABLE IF EXISTS phjf_probe;

CREATE TABLE phjf_build (k Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO phjf_build SELECT number FROM numbers(100);
INSERT INTO phjf_probe SELECT if(number % 50 = 0, NULL, toInt32(number % 250)) FROM numbers(20000);

SET join_algorithm = 'hash', max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

-- INNER + dense range -> shared filter fires.
SELECT 'inner_dense', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'inner_dense', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- build size = 1 -> guard skips, == const path stays.
SELECT 'inner_size1', count() FROM phjf_probe p INNER JOIN (SELECT 42 AS k) b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'inner_size1', count() FROM phjf_probe p INNER JOIN (SELECT 42 AS k) b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- LEFT JOIN -> joinRuntimeFilter does not run, NULL rows preserved.
SELECT 'left_dense', count(), sum(p.k IS NULL), sum(b.k IS NULL) FROM phjf_probe p LEFT JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'left_dense', count(), sum(p.k IS NULL), sum(b.k IS NULL) FROM phjf_probe p LEFT JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- LEFT SEMI -> filter fires, NULL probe never matches and is correctly dropped.
SELECT 'left_semi', count() FROM phjf_probe p LEFT SEMI JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'left_semi', count() FROM phjf_probe p LEFT SEMI JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- LEFT ANTI -> guard skips publish (NOT IN semantics), OR isNull(key) keeps NULL probes.
SELECT 'left_anti', count(), sum(p.k IS NULL) FROM phjf_probe p LEFT ANTI JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'left_anti', count(), sum(p.k IS NULL) FROM phjf_probe p LEFT ANTI JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- Multi-equi-condition AND -> keys128 path (not range_type), filter does not publish.
DROP TABLE phjf_build;
DROP TABLE phjf_probe;
CREATE TABLE phjf_build (k1 Int16, k2 Int16) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k1 Int16, k2 Int16) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO phjf_build SELECT toInt16(number % 50), toInt16(number % 50) FROM numbers(50);
INSERT INTO phjf_probe SELECT toInt16(number % 100), toInt16(number % 100) FROM numbers(2000);
SELECT 'multi_cond', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k1 = b.k1 AND p.k2 = b.k2 SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'multi_cond', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k1 = b.k1 AND p.k2 = b.k2 SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- Signed Int32 with negative keys -> bit-cast correctness in probe loop.
DROP TABLE phjf_build;
DROP TABLE phjf_probe;
CREATE TABLE phjf_build (k Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO phjf_build SELECT number - 50 FROM numbers(100);                  -- range [-50, 49]
INSERT INTO phjf_probe SELECT toInt32(number % 200 - 100) FROM numbers(20000);
SELECT 'signed_neg', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'signed_neg', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- Build has NULL keys -> HashJoin's FixedHashMap excludes them, baseline Set may include them; JOIN output stays identical.
DROP TABLE phjf_build;
DROP TABLE phjf_probe;
CREATE TABLE phjf_build (k Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_nullable_key = 1;
CREATE TABLE phjf_probe (k Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO phjf_build SELECT if(number % 20 = 0, NULL, toInt32(number)) FROM numbers(100);
INSERT INTO phjf_probe SELECT if(number % 30 = 0, NULL, toInt32(number % 200)) FROM numbers(20000);
SELECT 'null_in_build', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'null_in_build', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- UInt8 single-column join -> key8 path (FixedHashMap from build, no conversion needed).
DROP TABLE phjf_build;
DROP TABLE phjf_probe;
CREATE TABLE phjf_build (k UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO phjf_build SELECT toUInt8(number) FROM numbers(50);
INSERT INTO phjf_probe SELECT toUInt8(number % 100) FROM numbers(2000);
SELECT 'key8', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'key8', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- UInt16 single-column join -> key16 path.
DROP TABLE phjf_build;
DROP TABLE phjf_probe;
CREATE TABLE phjf_build (k UInt16) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k UInt16) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO phjf_build SELECT toUInt16(number) FROM numbers(1000);
INSERT INTO phjf_probe SELECT toUInt16(number % 2000) FROM numbers(20000);
SELECT 'key16', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'key16', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- Int8 with negative values -> key8 path with bit-cast through Int8.
DROP TABLE phjf_build;
DROP TABLE phjf_probe;
CREATE TABLE phjf_build (k Int8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k Int8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO phjf_build SELECT toInt8(number - 50) FROM numbers(100);
INSERT INTO phjf_probe SELECT toInt8(number % 200 - 100) FROM numbers(20000);
SELECT 'key8_signed', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'key8_signed', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- Range too wide for FixedHashMap conversion -> fallback to Set/BF.
DROP TABLE phjf_build;
DROP TABLE phjf_probe;
CREATE TABLE phjf_build (k Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO phjf_build SELECT number * 500000 FROM numbers(100);
INSERT INTO phjf_probe SELECT toInt32(number * 500000) FROM numbers(50);
SELECT 'inner_wide_range', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'inner_wide_range', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- Cross-type: UInt8 build + Int32 probe -> bounds check must drop probe values outside [0, 255].
DROP TABLE phjf_build;
DROP TABLE phjf_probe;
CREATE TABLE phjf_build (k UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO phjf_build SELECT toUInt8(number) FROM numbers(50);
INSERT INTO phjf_probe SELECT toInt32(number - 100) FROM numbers(2000);
SELECT 'cross_uint8_int32', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'cross_uint8_int32', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- Cross-type: Int8 build + Int64 probe -> signed narrow + reinterpret.
DROP TABLE phjf_build;
DROP TABLE phjf_probe;
CREATE TABLE phjf_build (k Int8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO phjf_build SELECT toInt8(number - 50) FROM numbers(100);
INSERT INTO phjf_probe SELECT toInt64(number - 500) FROM numbers(2000);
SELECT 'cross_int8_int64', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'cross_int8_int64', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- Cross-type: Int32 build + Int128 probe -> exercises ColumnVector<Int128> dispatch.
DROP TABLE phjf_build;
DROP TABLE phjf_probe;
CREATE TABLE phjf_build (k Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k Int128) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO phjf_build SELECT toInt32(number - 50) FROM numbers(100);
INSERT INTO phjf_probe SELECT toInt128(number - 100) FROM numbers(2000);
SELECT 'cross_int32_int128', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'cross_int32_int128', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- Int64 build -> range_*_key64 dispatch.
DROP TABLE phjf_build;
DROP TABLE phjf_probe;
CREATE TABLE phjf_build (k Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO phjf_build SELECT toInt64(number - 50) FROM numbers(100);
INSERT INTO phjf_probe SELECT toInt64(number % 200 - 100) FROM numbers(20000);
SELECT 'key64_signed', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'key64_signed', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- UInt64 build -> range_*_key64 dispatch (unsigned).
DROP TABLE phjf_build;
DROP TABLE phjf_probe;
CREATE TABLE phjf_build (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO phjf_build SELECT toUInt64(number) FROM numbers(100);
INSERT INTO phjf_probe SELECT toUInt64(number % 200) FROM numbers(20000);
SELECT 'key64_unsigned', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'key64_unsigned', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- OR clause -> maps.size() > 1 guard skips publish.
DROP TABLE phjf_build;
DROP TABLE phjf_probe;
CREATE TABLE phjf_build (k1 Int32, k2 Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k1 Int32, k2 Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO phjf_build SELECT toInt32(number), toInt32(number + 1000) FROM numbers(100);
INSERT INTO phjf_probe SELECT toInt32(number % 200), toInt32(number % 200 + 1000) FROM numbers(2000);
SELECT 'or_clause', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k1 = b.k1 OR p.k2 = b.k2 SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'or_clause', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k1 = b.k1 OR p.k2 = b.k2 SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- Date build -> key16 path via isValueRepresentedByInteger generalization.
DROP TABLE phjf_build;
DROP TABLE phjf_probe;
CREATE TABLE phjf_build (k Date) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k Date) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO phjf_build SELECT toDate('2024-01-01') + number FROM numbers(50);
INSERT INTO phjf_probe SELECT toDate('2024-01-01') + (number % 100) FROM numbers(2000);
SELECT 'date_key', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'date_key', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

DROP TABLE phjf_build;
DROP TABLE phjf_probe;
