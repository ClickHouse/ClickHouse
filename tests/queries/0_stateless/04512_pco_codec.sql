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
FROM numbers(20000);

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

-- Backed types accepted via their underlying integer representation: the type mapper treats
-- Decimal32/Decimal64 and DateTime64 as signed, IPv4 as unsigned, Enum as signed, Date32 as signed.
DROP TABLE IF EXISTS t_pco_backed;
CREATE TABLE t_pco_backed
(
    id    UInt64,
    dec32 Decimal32(4) CODEC(PCO),
    dec64 Decimal64(8) CODEC(PCO),
    ip4   IPv4 CODEC(PCO),
    e8    Enum8('a' = 1, 'b' = 2, 'c' = 3) CODEC(PCO),
    e16   Enum16('x' = -300, 'y' = 5, 'z' = 1000) CODEC(PCO),
    d32   Date32 CODEC(PCO),
    dt64  DateTime64(3) CODEC(PCO)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_pco_backed
SELECT
    number,
    toDecimal32(toInt32(number % 100000) - 50000, 4),
    toDecimal64(toInt64(number) * 1000 - 5, 8),
    CAST(toUInt32(number * 97 + 12345) AS IPv4),
    CAST(toInt8(number % 3 + 1) AS Enum8('a' = 1, 'b' = 2, 'c' = 3)),
    CAST([-300, 5, 1000][number % 3 + 1] AS Enum16('x' = -300, 'y' = 5, 'z' = 1000)),
    toDate32('1950-01-01') + (number % 20000),
    addMilliseconds(toDateTime64('2000-01-01 00:00:00', 3), number * 123)
FROM numbers(20000);

SELECT 'Decimal32',  sum(dec32 != toDecimal32(toInt32(id % 100000) - 50000, 4)) FROM t_pco_backed;
SELECT 'Decimal64',  sum(dec64 != toDecimal64(toInt64(id) * 1000 - 5, 8)) FROM t_pco_backed;
SELECT 'IPv4',       sum(ip4 != CAST(toUInt32(id * 97 + 12345) AS IPv4)) FROM t_pco_backed;
SELECT 'Enum8',      sum(toInt8(e8) != toInt8(id % 3 + 1)) FROM t_pco_backed;
SELECT 'Enum16',     sum(toInt16(e16) != [-300, 5, 1000][id % 3 + 1]) FROM t_pco_backed;
SELECT 'Date32',     sum(d32 != toDate32('1950-01-01') + (id % 20000)) FROM t_pco_backed;
SELECT 'DateTime64', sum(dt64 != addMilliseconds(toDateTime64('2000-01-01 00:00:00', 3), id * 123)) FROM t_pco_backed;

-- 16/32-byte and non-numeric types are rejected.
CREATE TABLE t_pco_bad (x Decimal128(10) CODEC(PCO)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_pco_bad (x Decimal256(10) CODEC(PCO)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_pco_bad (x String CODEC(PCO)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Codec chaining (Delta then PCO) and an explicit compression level.
DROP TABLE IF EXISTS t_pco_chain;
CREATE TABLE t_pco_chain (id UInt64, a Int64 CODEC(Delta, PCO), b UInt32 CODEC(PCO(12))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_pco_chain SELECT number, number * 3 - 7, toUInt32(number % 1000) FROM numbers(20000);
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
-- The compression-block layout (wide part, granularity, block size) is pinned on both tables so the
-- ratio is comparable regardless of the randomized MergeTree/session settings the flaky check applies:
-- otherwise tiny compression blocks (e.g. a compact part with `index_granularity = 3`) make every
-- codec's per-block framing dominate, and PCO's fixed per-chunk metadata can exceed NONE's.
DROP TABLE IF EXISTS t_pco_ratio;
DROP TABLE IF EXISTS t_none_ratio;
CREATE TABLE t_pco_ratio (x Int64 CODEC(PCO)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_compress_block_size = 1048576, max_compress_block_size = 1048576;
CREATE TABLE t_none_ratio (x Int64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_compress_block_size = 1048576, max_compress_block_size = 1048576;
INSERT INTO t_pco_ratio SELECT number * 2 - 5 FROM numbers(100000);
INSERT INTO t_none_ratio SELECT number * 2 - 5 FROM numbers(100000);
SELECT 'compresses', (SELECT sum(data_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 't_pco_ratio' AND active)
                   < (SELECT sum(data_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 't_none_ratio' AND active);

DROP TABLE t_pco;
DROP TABLE t_pco_backed;
DROP TABLE t_pco_chain;
DROP TABLE t_pco_small;
DROP TABLE t_pco_ratio;
DROP TABLE t_none_ratio;
