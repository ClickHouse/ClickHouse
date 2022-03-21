SET allow_experimental_bigint_types=1;

CREATE TABLE IF NOT EXISTS test_01035_avg (
    i8 Int8         DEFAULT i64,
    i16 Int16       DEFAULT i64,
    i32 Int32       DEFAULT i64,
    i64 Int64       DEFAULT if(u64 % 2 = 0, toInt64(u64), toInt64(-u64)),
    i128 Int128     DEFAULT i64,
    i256 Int256     DEFAULT i64,

    u8 UInt8        DEFAULT u64,
    u16 UInt16      DEFAULT u64,
    u32 UInt32      DEFAULT u64,
    u64 UInt64,
    u128 UInt128    DEFAULT u64,
    u256 UInt256    DEFAULT u64,

    f32 Float32     DEFAULT u64,
    f64 Float64     DEFAULT u64,

    d32 Decimal32(4)    DEFAULT toDecimal32(i32 / 1000, 4),
    d64 Decimal64(18)   DEFAULT toDecimal64(u64 / 1000000, 8),
    d128 Decimal128(20) DEFAULT toDecimal128(i128 / 100000, 20),
    d256 Decimal256(40) DEFAULT toDecimal256(i256 / 100000, 40)
) ENGINE = MergeTree() ORDER BY i64;

SELECT avg(i8), avg(i16), avg(i32), avg(i64), avg(i128), avg(i256),
       avg(u8), avg(u16), avg(u32), avg(u64), avg(u128), avg(u256),
       avg(f32), avg(f64),
       avg(d32), avg(d64), avg(d128), avg(d256) FROM test_01035_avg;

INSERT INTO test_01035_avg (u64) SELECT number FROM system.numbers LIMIT 1000000;

SELECT avg(i8), avg(i16), avg(i32), avg(i64), avg(i128), avg(i256),
       avg(u8), avg(u16), avg(u32), avg(u64), avg(u128), avg(u256),
       avg(f32), avg(f64),
       avg(d32), avg(d64), avg(d128), avg(d256) FROM test_01035_avg;

SELECT avg(i8 * i16) FROM test_01035_avg;
SELECT avg(f32 + f64) FROM test_01035_avg;
SELECT avg(d128 - d64) FROM test_01035_avg;

DROP TABLE IF EXISTS test_01035_avg;

-- Checks that the internal SUM does not overflow Int8
SELECT avg(key), avgIf(key, key > 0), avg(key2), avgIf(key2, key2 > 0), avg(key3), avgIf(key3, key3 > 0)
FROM
(
     SELECT 1::Int8 as key, Null::Nullable(Int8) AS key2, 1::Nullable(Int8) as key3
     FROM numbers(100000)
)
