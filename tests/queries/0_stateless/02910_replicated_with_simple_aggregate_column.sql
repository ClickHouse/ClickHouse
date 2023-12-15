CREATE TABLE t_r1
(
    `id` UInt64,
    `val` SimpleAggregateFunction(max, Nullable(String))
)
ENGINE = ReplicatedAggregatingMergeTree('/tables/{database}/t', 'r1')
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE t_r2
(
    `id` UInt64,
    `val` SimpleAggregateFunction(anyLast, Nullable(String))
)
ENGINE = ReplicatedAggregatingMergeTree('/tables/{database}/t', 'r2')
ORDER BY id
SETTINGS index_granularity = 8192; -- { serverError INCOMPATIBLE_COLUMNS }

DROP TABLE IF EXISTS t_r1;

CREATE TABLE t2_r1
(
    `id` UInt64,
    `val` AggregateFunction(uniqCombined(19), UInt64)
)
ENGINE = ReplicatedAggregatingMergeTree('/tables/{database}/t2', 'r1')
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE t2_r2
(
    `id` UInt64,
    `val` AggregateFunction(uniqCombined(17), UInt64)
)
ENGINE = ReplicatedAggregatingMergeTree('/tables/{database}/t2', 'r2')
ORDER BY id
SETTINGS index_granularity = 8192; -- { serverError INCOMPATIBLE_COLUMNS }

DROP TABLE IF EXISTS t2_r1;


-- Test identical method for all types

SET allow_suspicious_low_cardinality_types=1;
SET allow_experimental_object_type=1;

CREATE TABLE t_all_numeric_types_r1
(
    i8            Int8,
    i16           Int16,
    i32           Int32,
    i64           Int64,
    i128          Int128,
    i256          Int256,
    ui8           UInt8,
    ui16          UInt16,
    ui32          UInt32,
    ui64          UInt64,
    ui128         UInt128,
    ui256         UInt256,
    f32           Float32,
    f64           Float64
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_numeric_types', 'r1')
ORDER BY tuple();

CREATE TABLE t_all_numeric_types_r2
(
    i8            Int8,
    i16           Int16,
    i32           Int32,
    i64           Int64,
    i128          Int128,
    i256          Int256,
    ui8           UInt8,
    ui16          UInt16,
    ui32          UInt32,
    ui64          UInt64,
    ui128         UInt128,
    ui256         UInt256,
    f32           Float32,
    f64           Float64
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_numeric_types', 'r2')
ORDER BY tuple();

DROP TABLE t_all_numeric_types_r1;
DROP TABLE t_all_numeric_types_r2;

CREATE TABLE t_all_decimal_types_r1
(
    dec32         Decimal32(2),
    dec64         Decimal64(2),
    dec128        Decimal128(2),
    dec128_native Decimal(35, 30),
    dec128_text   Decimal(35, 31),
    dec256        Decimal256(2),
    dec256_native Decimal(65, 2),
    dec256_text   Decimal(66, 2)
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_decimal_types', 'r1')
ORDER BY tuple();

CREATE TABLE t_all_decimal_types_r2
(
    dec32         Decimal32(2),
    dec64         Decimal64(2),
    dec128        Decimal128(2),
    dec128_native Decimal(35, 30),
    dec128_text   Decimal(35, 31),
    dec256        Decimal256(2),
    dec256_native Decimal(65, 2),
    dec256_text   Decimal(66, 2)
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_decimal_types', 'r2')
ORDER BY tuple();

DROP TABLE IF EXISTS t_all_decimal_types_r1;
DROP TABLE IF EXISTS t_all_decimal_types_r2;

CREATE TABLE t_all_geo_types_r1
(
    p             Point,
    r             Ring,
    pg            Polygon,
    mpg           MultiPolygon
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_geo_types', 'r1')
ORDER BY tuple();

CREATE TABLE t_all_geo_types_r2
(
    p             Point,
    r             Ring,
    pg            Polygon,
    mpg           MultiPolygon
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_geo_types', 'r2')
ORDER BY tuple();

DROP TABLE IF EXISTS t_all_geo_types_r1;
DROP TABLE IF EXISTS t_all_geo_types_r2;

CREATE TABLE t_all_string_types_r1
(
    s             String,
    fs            FixedString(3)
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_string_types', 'r1')
ORDER BY tuple();

CREATE TABLE t_all_string_types_r2
(
    s             String,
    fs            FixedString(3)
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_string_types', 'r2')
ORDER BY tuple();

DROP TABLE IF EXISTS t_all_string_types_r1;
DROP TABLE IF EXISTS t_all_string_types_r2;

CREATE TABLE t_all_time_types_r1
(
    d             Date,
    d32           Date32,
    dt            DateTime,
    dt_tz1        DateTime('UTC'),
    dt_tz2        DateTime('Europe/Amsterdam'),
    dt64          DateTime64(3),
    dt64_3_tz1    DateTime64(3, 'UTC'),
    dt64_3_tz2    DateTime64(3, 'Asia/Shanghai'),
    dt64_6        DateTime64(6, 'UTC'),
    dt64_9        DateTime64(9, 'UTC')
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_time_types', 'r1')
ORDER BY tuple();

CREATE TABLE t_all_time_types_r2
(
    d             Date,
    d32           Date32,
    dt            DateTime,
    dt_tz1        DateTime('UTC'),
    dt_tz2        DateTime('Europe/Amsterdam'),
    dt64          DateTime64(3),
    dt64_3_tz1    DateTime64(3, 'UTC'),
    dt64_3_tz2    DateTime64(3, 'Asia/Shanghai'),
    dt64_6        DateTime64(6, 'UTC'),
    dt64_9        DateTime64(9, 'UTC'),
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_time_types', 'r2')
ORDER BY tuple();

DROP TABLE IF EXISTS t_all_time_types_r1;
DROP TABLE IF EXISTS t_all_time_types_r2;

CREATE TABLE t_all_agg_types_r1
(
    agg1           AggregateFunction(uniqCombined(13), UInt64),
    agg2           AggregateFunction(uniq, UInt64),
    sagg1          SimpleAggregateFunction(sum, Double),
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_agg_types', 'r1')
ORDER BY tuple();

CREATE TABLE t_all_agg_types_r2
(
    agg1           AggregateFunction(uniqCombined(13), UInt64),
    agg2           AggregateFunction(uniq, UInt64),
    sagg1          SimpleAggregateFunction(sum, Double),
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_agg_types', 'r2')
ORDER BY tuple();

DROP TABLE IF EXISTS t_all_agg_types_r1;
DROP TABLE IF EXISTS t_all_agg_types_r2;


CREATE TABLE t_all_complex_types_r1
(
    a             Array(String),
    o             JSON,
    t             Tuple(Int32, String, Nullable(String), LowCardinality(String), LowCardinality(Nullable(String)), Tuple(Int32, String)),
    m             Map(Int32, String),
    m_complex     Map(Int32, Map(Int32, LowCardinality(Nullable(String)))),
    nested        Nested (col1 String, col2 UInt32)
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_complex_types', 'r1')
ORDER BY tuple();

CREATE TABLE t_all_complex_types_r2
(
    a             Array(String),
    o             JSON,
    t             Tuple(Int32, String, Nullable(String), LowCardinality(String), LowCardinality(Nullable(String)), Tuple(Int32, String)),
    m             Map(Int32, String),
    m_complex     Map(Int32, Map(Int32, LowCardinality(Nullable(String)))),
    nested        Nested (col1 String, col2 UInt32)
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_complex_types', 'r2')
ORDER BY tuple();

DROP TABLE IF EXISTS t_all_complex_types_r1;
DROP TABLE IF EXISTS t_all_complex_types_r2;

CREATE TABLE t_all_nullable_types_r1
(
    ns            Nullable(String),
    nfs           Nullable(FixedString(3)),
    ndt64         Nullable(DateTime64(3)),
    ndt64_tz      Nullable(DateTime64(3, 'Asia/Shanghai'))
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_nullable_types', 'r1')
ORDER BY tuple();

CREATE TABLE t_all_nullable_types_r2
(
    ns            Nullable(String),
    nfs           Nullable(FixedString(3)),
    ndt64         Nullable(DateTime64(3)),
    ndt64_tz      Nullable(DateTime64(3, 'Asia/Shanghai'))
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_nullable_types', 'r2')
ORDER BY tuple();

DROP TABLE IF EXISTS t_all_nullable_types_r1;
DROP TABLE IF EXISTS t_all_nullable_types_r2;

CREATE TABLE t_all_lc_types_r1
(
    ls            LowCardinality(String),
    lfs           LowCardinality(FixedString(3)),
    lns           LowCardinality(Nullable(String)),
    lnfs          LowCardinality(Nullable(FixedString(3))),
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_lc_types', 'r1')
ORDER BY tuple();

CREATE TABLE t_all_lc_types_r2
(
    ls            LowCardinality(String),
    lfs           LowCardinality(FixedString(3)),
    lns           LowCardinality(Nullable(String)),
    lnfs          LowCardinality(Nullable(FixedString(3))),
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_lc_types', 'r2')
ORDER BY tuple();

DROP TABLE IF EXISTS t_all_lc_types_r1;
DROP TABLE IF EXISTS t_all_lc_types_r2;

CREATE TABLE t_all_other_types_r1
(
    uuid          UUID,
    ip4           IPv4,
    ip6           IPv6,
    b             Bool,
    enm           Enum('hallo' = 1, 'welt' = 2)
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_other_types', 'r1')
ORDER BY tuple();

CREATE TABLE t_all_other_types_r2
(
    uuid          UUID,
    ip4           IPv4,
    ip6           IPv6,
    b             Bool,
    enm           Enum('hallo' = 1, 'welt' = 2)
)
ENGINE = ReplicatedMergeTree('/tables/{database}/t_all_other_types', 'r2')
ORDER BY tuple();

DROP TABLE IF EXISTS t_all_other_types_r1;
DROP TABLE IF EXISTS t_all_other_types_r2;