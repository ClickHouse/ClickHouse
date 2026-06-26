DROP TABLE IF EXISTS source_04411;
DROP TABLE IF EXISTS horizontal_default_04411;
DROP TABLE IF EXISTS horizontal_batch_04411;
DROP TABLE IF EXISTS vertical_default_04411;
DROP TABLE IF EXISTS vertical_batch_04411;

CREATE TABLE source_04411
(
    id UInt64,
    k UInt32,
    subkey UInt32,
    u8 UInt8,
    i64 Int64,
    f64 Float64,
    dec Decimal(18, 4),
    s String,
    fs FixedString(12),
    d Date,
    dt DateTime,
    dt64 DateTime64(3),
    uuid UUID,
    ip4 IPv4,
    ip6 IPv6,
    e Enum8('a' = 1, 'b' = 2, 'c' = 3),
    lc LowCardinality(String),
    n Nullable(Int32),
    arr Array(UInt16),
    arr_nullable Array(Nullable(Int32)),
    tup Tuple(UInt32, String, Nullable(Int16)),
    m Map(String, UInt32)
)
ENGINE = Memory;

INSERT INTO source_04411
SELECT
    number AS id,
    toUInt32(number % 17) AS k,
    toUInt32(intHash32(number) % 11) AS subkey,
    toUInt8(number % 255) AS u8,
    toInt64(number) * -3 AS i64,
    toFloat64(number % 1000) / 7.0 AS f64,
    toDecimal64(toInt64(number % 100000) - 50000, 4) AS dec,
    concat('str-', toString(number % 37), '-', toString(intHash32(number))) AS s,
    toFixedString(concat('fs', leftPad(toString(number % 10000000000), 10, '0')), 12) AS fs,
    toDate('2024-01-01') + toUInt16(number % 31) AS d,
    toDateTime('2024-01-01 00:00:00') + toIntervalSecond(number % 86400) AS dt,
    toDateTime64(toDateTime('2024-01-01 00:00:00') + toIntervalSecond(number % 86400), 3) AS dt64,
    toUUID('00000000-0000-0000-0000-000000000001') AS uuid,
    toIPv4(concat('10.0.', toString(number % 255), '.', toString((number * 7) % 255))) AS ip4,
    toIPv6('2001:db8::1') AS ip6,
    CAST(multiIf(number % 3 = 0, 'a', number % 3 = 1, 'b', 'c'), 'Enum8(''a'' = 1, ''b'' = 2, ''c'' = 3)') AS e,
    concat('lc-', toString(number % 13)) AS lc,
    if(number % 7 = 0, NULL, toInt32(number) - 100) AS n,
    arrayMap(x -> toUInt16((number + x) % 1000), range(toUInt8(number % 4))) AS arr,
    arrayMap(x -> if(x % 2 = 0, NULL, toInt32(number + x)), range(toUInt8(number % 5))) AS arr_nullable,
    (toUInt32(number % 101), concat('tuple-', toString(number % 19)), if(number % 5 = 0, NULL, toInt16(number % 128))) AS tup,
    map('x', toUInt32(number), 'y', toUInt32(intHash32(number) % 1000)) AS m
FROM numbers(240);

CREATE TABLE horizontal_default_04411 AS source_04411
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY (k, subkey)
SETTINGS
    merge_sorting_queue_strategy = 'default',
    enable_vertical_merge_algorithm = 0,
    index_granularity = 8,
    index_granularity_bytes = 0,
    merge_max_block_size = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

CREATE TABLE horizontal_batch_04411 AS source_04411
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY (k, subkey)
SETTINGS
    merge_sorting_queue_strategy = 'batch',
    enable_vertical_merge_algorithm = 0,
    index_granularity = 8,
    index_granularity_bytes = 0,
    merge_max_block_size = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

CREATE TABLE vertical_default_04411 AS source_04411
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY (k, subkey)
SETTINGS
    merge_sorting_queue_strategy = 'default',
    enable_vertical_merge_algorithm = 1,
    index_granularity = 8,
    index_granularity_bytes = 0,
    merge_max_block_size = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1;

CREATE TABLE vertical_batch_04411 AS source_04411
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY (k, subkey)
SETTINGS
    merge_sorting_queue_strategy = 'batch',
    enable_vertical_merge_algorithm = 1,
    index_granularity = 8,
    index_granularity_bytes = 0,
    merge_max_block_size = 64,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO horizontal_default_04411 SELECT * FROM source_04411 WHERE id % 3 = 0;
INSERT INTO horizontal_default_04411 SELECT * FROM source_04411 WHERE id % 3 = 1;
INSERT INTO horizontal_default_04411 SELECT * FROM source_04411 WHERE id % 3 = 2;

INSERT INTO horizontal_batch_04411 SELECT * FROM source_04411 WHERE id % 3 = 0;
INSERT INTO horizontal_batch_04411 SELECT * FROM source_04411 WHERE id % 3 = 1;
INSERT INTO horizontal_batch_04411 SELECT * FROM source_04411 WHERE id % 3 = 2;

INSERT INTO vertical_default_04411 SELECT * FROM source_04411 WHERE id % 3 = 0;
INSERT INTO vertical_default_04411 SELECT * FROM source_04411 WHERE id % 3 = 1;
INSERT INTO vertical_default_04411 SELECT * FROM source_04411 WHERE id % 3 = 2;

INSERT INTO vertical_batch_04411 SELECT * FROM source_04411 WHERE id % 3 = 0;
INSERT INTO vertical_batch_04411 SELECT * FROM source_04411 WHERE id % 3 = 1;
INSERT INTO vertical_batch_04411 SELECT * FROM source_04411 WHERE id % 3 = 2;

OPTIMIZE TABLE horizontal_default_04411 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE horizontal_batch_04411 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE vertical_default_04411 FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE vertical_batch_04411 FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT throwIf(
    (
        SELECT groupArray(tuple(*))
        FROM (SELECT * FROM horizontal_default_04411 ORDER BY k, subkey, id)
    ) != (
        SELECT groupArray(tuple(*))
        FROM (SELECT * FROM horizontal_batch_04411 ORDER BY k, subkey, id)
    ),
    'Horizontal merge results differ between default and batch sorting queue strategies')
FORMAT Null;

SELECT throwIf(
    (
        SELECT groupArray(tuple(*))
        FROM (SELECT * FROM vertical_default_04411 ORDER BY k, subkey, id)
    ) != (
        SELECT groupArray(tuple(*))
        FROM (SELECT * FROM vertical_batch_04411 ORDER BY k, subkey, id)
    ),
    'Vertical merge results differ between default and batch sorting queue strategies')
FORMAT Null;

SELECT 'horizontal ok';
SELECT 'vertical ok';

DROP TABLE horizontal_default_04411;
DROP TABLE horizontal_batch_04411;
DROP TABLE vertical_default_04411;
DROP TABLE vertical_batch_04411;
DROP TABLE source_04411;
