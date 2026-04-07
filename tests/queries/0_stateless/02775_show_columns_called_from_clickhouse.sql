-- Tags: no-fasttest, no-parallel
-- no-fasttest: json type needs rapidjson library, geo types need s2 geometry
-- no-parallel: can't provide currentDatabase() to SHOW COLUMNS

-- Tests the output of SHOW COLUMNS when called through the ClickHouse protocol.

-- -----------------------------------------------------------------------------------
-- Please keep this test in-sync with 02775_show_columns_called_from_clickhouse.expect
-- -----------------------------------------------------------------------------------

DROP TABLE IF EXISTS tab;

SET allow_suspicious_low_cardinality_types=1;
SET enable_json_type=1;

CREATE TABLE tab
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
    f64           Float64,
    dec32         Decimal32(2),
    dec64         Decimal64(2),
    dec128        Decimal128(2),
    dec128_native Decimal(35, 30),
    dec128_text   Decimal(35, 31),
    dec256        Decimal256(2),
    dec256_native Decimal(65, 2),
    dec256_text   Decimal(66, 2),
    p             Point,
    r             Ring,
    pg            Polygon,
    mpg           MultiPolygon,
    b             Bool,
    s             String,
    fs            FixedString(3),
    uuid          UUID,
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
    enm           Enum('hallo' = 1, 'welt' = 2),
    agg           AggregateFunction(uniq, UInt64),
    sagg          SimpleAggregateFunction(sum, Double),
    a             Array(String),
    o             JSON,
    t             Tuple(Int32, String, Nullable(String), LowCardinality(String), LowCardinality(Nullable(String)), Tuple(Int32, String)),
    m             Map(Int32, String),
    m_complex     Map(Int32, Map(Int32, LowCardinality(Nullable(String)))),
    nested        Nested (col1 String, col2 UInt32),
    ip4           IPv4,
    ip6           IPv6,
    ns            Nullable(String),
    nfs           Nullable(FixedString(3)),
    ndt64         Nullable(DateTime64(3)),
    ndt64_tz      Nullable(DateTime64(3, 'Asia/Shanghai')),
    ls            LowCardinality(String),
    lfs           LowCardinality(FixedString(3)),
    lns           LowCardinality(Nullable(String)),
    lnfs          LowCardinality(Nullable(FixedString(3))),
) ENGINE Memory;

SHOW COLUMNS FROM tab;

DROP TABLE tab;
