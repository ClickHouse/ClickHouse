-- Tags: no-fasttest, no-parallel
-- no-fasttest: json type needs rapidjson library
-- no-parallel: can't provide currentDatabase() to SHOW COLUMNS

-- Tests setting 'use_mysql_types_in_show_columns' in SHOW COLUMNS and SELECTs on system.columns

DROP TABLE IF EXISTS tab;

SET allow_suspicious_low_cardinality_types=1;
SET allow_experimental_object_type=1;

CREATE TABLE tab
(
    uint8 UInt8,
    uint16 UInt16,
    uint32 UInt32,
    uint64 UInt64,
    uint128 UInt128,
    uint256 UInt256,
    int8 Int8,
    int16 Int16,
    int32 Int32,
    int64 Int64,
    int128 Int128,
    int256 Int256,
    nint32 Nullable(Int32),
    float32 Float32,
    float64 Float64,
    decimal_value Decimal(10, 2),
    boolean_value UInt8,
    string_value String,
    fixed_string_value FixedString(10),
    date_value Date,
    date32_value Date32,
    datetime_value DateTime,
    datetime64_value DateTime64(3),
    json_value JSON,
    uuid_value UUID,
    enum_value Enum8('apple' = 1, 'banana' = 2, 'orange' = 3),
    low_cardinality LowCardinality(String),
    low_cardinality_date LowCardinality(DateTime),
    aggregate_function AggregateFunction(sum, Int32),
    array_value Array(Int32),
    map_value Map(String, Int32),
    tuple_value Tuple(Int32, String),
    nullable_value Nullable(Int32),
    ipv4_value IPv4,
    ipv6_value IPv6,
    nested Nested
    (
        nested_int Int32,
        nested_string String
    )
) ENGINE = MergeTree
ORDER BY uint64;

SHOW COLUMNS FROM tab SETTINGS use_mysql_types_in_show_columns = 0;
SHOW COLUMNS FROM tab SETTINGS use_mysql_types_in_show_columns = 1;

SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'tab' SETTINGS use_mysql_types_in_show_columns = 0;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'tab' SETTINGS use_mysql_types_in_show_columns = 1;

DROP TABLE tab;
