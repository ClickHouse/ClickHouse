-- Tags: no-fasttest
-- Tests the PCO (pcodec) compression codec: lossless round-trip across numeric types, codec
-- chaining, parameters, and that it actually compresses. Each check prints 0 = no mismatches.

SET allow_experimental_codecs = 1;

-- Per-element round-trip verification for every supported numeric type.
DROP TABLE IF EXISTS t_pco;
CREATE TABLE t_pco
(
    id     UInt64,
    i8     Int8    CODEC(PCO),
    i16    Int16   CODEC(PCO),
    i32    Int32   CODEC(PCO),
    i64    Int64   CODEC(PCO),
    u8     UInt8   CODEC(PCO),
    u16    UInt16  CODEC(PCO),
    u32    UInt32  CODEC(PCO),
    u64    UInt64  CODEC(PCO),
    f32    Float32 CODEC(PCO),
    f64    Float64 CODEC(PCO),
    d      Date    CODEC(PCO),
    dt     DateTime CODEC(PCO)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_pco
SELECT
    number,
    toInt8(number % 251 - 125),
    toInt16(number % 60000 - 30000),
    toInt32(number * 7 - 11),
    toInt64(number * 1000000 - 500),
    toUInt8(number % 256),
    toUInt16(number % 65000),
    toUInt32(number * 13),
    toUInt64(number * 1000000007),
    toFloat32(number) / 32,
    toFloat64(number) / 7,
    toDate('2000-01-01') + (number % 10000),
    toDateTime('2000-01-01 00:00:00') + number
FROM numbers(100000);

SELECT 'Int8',    sum(i8  != toInt8(id % 251 - 125)) FROM t_pco;
SELECT 'Int16',   sum(i16 != toInt16(id % 60000 - 30000)) FROM t_pco;
SELECT 'Int32',   sum(i32 != toInt32(id * 7 - 11)) FROM t_pco;
SELECT 'Int64',   sum(i64 != toInt64(id * 1000000 - 500)) FROM t_pco;
SELECT 'UInt8',   sum(u8  != toUInt8(id % 256)) FROM t_pco;
SELECT 'UInt16',  sum(u16 != toUInt16(id % 65000)) FROM t_pco;
SELECT 'UInt32',  sum(u32 != toUInt32(id * 13)) FROM t_pco;
SELECT 'UInt64',  sum(u64 != toUInt64(id * 1000000007)) FROM t_pco;
SELECT 'Float32', sum(f32 != toFloat32(id) / 32) FROM t_pco;
SELECT 'Float64', sum(f64 != toFloat64(id) / 7) FROM t_pco;
SELECT 'Date',    sum(d   != toDate('2000-01-01') + (id % 10000)) FROM t_pco;
SELECT 'DateTime',sum(dt  != toDateTime('2000-01-01 00:00:00') + id) FROM t_pco;

-- Survives a merge.
OPTIMIZE TABLE t_pco FINAL;
SELECT 'after_merge', sum(i64 != toInt64(id * 1000000 - 500)) FROM t_pco;

-- Codec chaining (Delta then PCO) and an explicit compression level.
DROP TABLE IF EXISTS t_pco_chain;
CREATE TABLE t_pco_chain (id UInt64, a Int64 CODEC(Delta, PCO), b UInt32 CODEC(PCO(12))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_pco_chain SELECT number, number * 3 - 7, toUInt32(number % 1000) FROM numbers(100000);
SELECT 'chain_a', sum(a != toInt64(id * 3 - 7)) FROM t_pco_chain;
SELECT 'chain_b', sum(b != toUInt32(id % 1000)) FROM t_pco_chain;

-- Edge cases: tiny and single-row tables.
DROP TABLE IF EXISTS t_pco_small;
CREATE TABLE t_pco_small (id UInt64, a Int32 CODEC(PCO)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_pco_small SELECT number, toInt32(number) - 2 FROM numbers(3);
SELECT 'small', sum(a != toInt32(id) - 2) FROM t_pco_small;
INSERT INTO t_pco_small VALUES (100, 999);
SELECT 'single', (SELECT a FROM t_pco_small WHERE id = 100);

-- PCO compresses sequential data far better than NONE.
DROP TABLE IF EXISTS t_pco_ratio;
DROP TABLE IF EXISTS t_none_ratio;
CREATE TABLE t_pco_ratio (x Int64 CODEC(PCO)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_none_ratio (x Int64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_pco_ratio SELECT number * 2 - 5 FROM numbers(500000);
INSERT INTO t_none_ratio SELECT number * 2 - 5 FROM numbers(500000);
SELECT 'compresses', (SELECT sum(data_compressed_bytes) FROM system.parts WHERE table = 't_pco_ratio' AND active)
                   < (SELECT sum(data_compressed_bytes) FROM system.parts WHERE table = 't_none_ratio' AND active);

DROP TABLE t_pco;
DROP TABLE t_pco_chain;
DROP TABLE t_pco_small;
DROP TABLE t_pco_ratio;
DROP TABLE t_none_ratio;
