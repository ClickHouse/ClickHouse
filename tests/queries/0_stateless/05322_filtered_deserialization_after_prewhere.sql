-- Columns read after PREWHERE are deserialized only for the rows that passed it (`filtered_deserialization_max_ratio`).
-- Every query is run with the optimization disabled and forced, the results must be equal.

SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 0;
-- The columns cache holds whole granules, the columns read through it are not filtered (tested separately below).
SET use_columns_cache = 0;

DROP TABLE IF EXISTS t_filtered_deserialization;

CREATE TABLE t_filtered_deserialization
(
    id UInt64,
    u8 UInt8,
    i32 Int32,
    f64 Float64,
    dec Decimal(38, 5),
    dt64 DateTime64(3, 'UTC'),
    d Date,
    e Enum8('a' = 1, 'b' = 2, 'c' = 3),
    b Bool,
    uuid UUID,
    ip6 IPv6,
    fs FixedString(5),
    s String,
    ns Nullable(String),
    ni Nullable(Int64),
    arr Array(UInt32),
    arr_str Array(String),
    arr_arr Array(Array(Int16)),
    arr_nullable Array(Nullable(String)),
    tup Tuple(a String, b Array(Decimal(18, 2)), c Nullable(UInt8)),
    m Map(String, Array(UInt8)),
    n Nested(x UInt32, y String),
    lc LowCardinality(String),
    arr_lc Array(LowCardinality(String)),
    sparse UInt64,
    dyn Dynamic
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_filtered_deserialization SELECT
    number,
    number % 256,
    toInt32(number * 7 - 50000),
    number / 3,
    toDecimal128(number, 5) / 7,
    toDateTime64('2020-01-01 00:00:00', 3, 'UTC') + number,
    toDate('2020-01-01') + number % 1000,
    ['a', 'b', 'c'][number % 3 + 1],
    number % 2 = 0,
    toUUID(concat('00000000-0000-0000-0000-', leftPad(toString(number), 12, '0'))),
    toIPv6(IPv4NumToString(toUInt32(number))),
    toFixedString(toString(number % 100000), 5),
    repeat(toString(number), number % 5),
    if(number % 4 = 0, NULL, toString(number)),
    if(number % 6 = 0, NULL, number),
    range(number % 7),
    arrayMap(x -> repeat('x', x), range(number % 4)),
    arrayMap(x -> range(x), range(number % 5)),
    arrayMap(x -> if(x % 2 = 0, NULL, toString(x)), range(number % 3)),
    (toString(number), arrayMap(x -> toDecimal64(x, 2), range(number % 3)), if(number % 5 = 0, NULL, number % 256)),
    mapFromArrays(arrayMap(x -> toString(x), range(number % 3)), arrayMap(x -> range(x), range(number % 3))),
    range(number % 4),
    arrayMap(x -> toString(x * number), range(number % 4)),
    toString(number % 10),
    arrayMap(x -> toString(x), range(number % 3)),
    if(number % 50 = 0, number, 0),
    multiIf(number % 3 = 0, number::Dynamic, number % 3 = 1, toString(number)::Dynamic, NULL)
FROM numbers(20000);

-- A column added after the data was written is read as defaults.
ALTER TABLE t_filtered_deserialization ADD COLUMN added Array(String) DEFAULT arrayMap(x -> toString(x), range(id % 3));

SELECT 'wide part', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_filtered_deserialization' AND active;

-- All columns, PREWHERE with different selectivity and distribution of the passed rows.
SELECT 'all 1%', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization PREWHERE id % 97 = 3 SETTINGS filtered_deserialization_max_ratio = 0, log_comment = '05322_disabled';
SELECT 'all 1%', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization PREWHERE id % 97 = 3 SETTINGS filtered_deserialization_max_ratio = 1, log_comment = '05322_enabled';
SELECT 'all 8%', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization PREWHERE id % 13 = 0 SETTINGS filtered_deserialization_max_ratio = 0;
SELECT 'all 8%', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization PREWHERE id % 13 = 0 SETTINGS filtered_deserialization_max_ratio = 1;
SELECT 'all 67%', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization PREWHERE id % 3 != 0 SETTINGS filtered_deserialization_max_ratio = 0;
SELECT 'all 67%', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization PREWHERE id % 3 != 0 SETTINGS filtered_deserialization_max_ratio = 1;
SELECT 'all runs', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization PREWHERE intDiv(id, 50) % 20 = 1 SETTINGS filtered_deserialization_max_ratio = 0;
SELECT 'all runs', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization PREWHERE intDiv(id, 50) % 20 = 1 SETTINGS filtered_deserialization_max_ratio = 1;
SELECT 'all granule edges', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization PREWHERE id % 64 IN (0, 63) SETTINGS filtered_deserialization_max_ratio = 0;
SELECT 'all granule edges', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization PREWHERE id % 64 IN (0, 63) SETTINGS filtered_deserialization_max_ratio = 1;
SELECT 'all none', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization PREWHERE u8 = 1000 SETTINGS filtered_deserialization_max_ratio = 0;
SELECT 'all none', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization PREWHERE u8 = 1000 SETTINGS filtered_deserialization_max_ratio = 1;

-- The default ratio.
SELECT 'default ratio', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization PREWHERE id % 97 = 3;

-- Subcolumns together with the full columns and the shared sizes of `Nested`.
SELECT 'subcolumns', count(), sum(cityHash64(toString((arr, arr.size0, s, s.size, ns.null, tup.a, tup.b, m.keys, m.values, n.x, n.y, n.x.size0, ni, ni.null))))
FROM t_filtered_deserialization PREWHERE id % 31 = 5 SETTINGS filtered_deserialization_max_ratio = 0;
SELECT 'subcolumns', count(), sum(cityHash64(toString((arr, arr.size0, s, s.size, ns.null, tup.a, tup.b, m.keys, m.values, n.x, n.y, n.x.size0, ni, ni.null))))
FROM t_filtered_deserialization PREWHERE id % 31 = 5 SETTINGS filtered_deserialization_max_ratio = 1;
SELECT 'nested', count(), sum(cityHash64(toString((n.x, n.y, tup.b, m)))) FROM t_filtered_deserialization PREWHERE id % 31 = 5 SETTINGS filtered_deserialization_max_ratio = 0;
SELECT 'nested', count(), sum(cityHash64(toString((n.x, n.y, tup.b, m)))) FROM t_filtered_deserialization PREWHERE id % 31 = 5 SETTINGS filtered_deserialization_max_ratio = 1;
SELECT 'only subcolumns', count(), sum(cityHash64(toString((arr.size0, tup.c, ns.null, s.size)))) FROM t_filtered_deserialization PREWHERE id % 31 = 5 SETTINGS filtered_deserialization_max_ratio = 0;
SELECT 'only subcolumns', count(), sum(cityHash64(toString((arr.size0, tup.c, ns.null, s.size)))) FROM t_filtered_deserialization PREWHERE id % 31 = 5 SETTINGS filtered_deserialization_max_ratio = 1;

-- Virtual columns.
SELECT 'virtual', count(), sum(cityHash64(toString((_part_offset, _part, arr, s)))) FROM t_filtered_deserialization PREWHERE id % 29 = 1 SETTINGS filtered_deserialization_max_ratio = 0;
SELECT 'virtual', count(), sum(cityHash64(toString((_part_offset, _part, arr, s)))) FROM t_filtered_deserialization PREWHERE id % 29 = 1 SETTINGS filtered_deserialization_max_ratio = 1;

-- Several PREWHERE steps.
SELECT 'steps', count(), sum(cityHash64(toString((arr_arr, s, dec, m)))) FROM t_filtered_deserialization PREWHERE id % 5 = 0 AND u8 % 7 = 1 AND length(arr_str) = 2
SETTINGS filtered_deserialization_max_ratio = 0, enable_multiple_prewhere_read_steps = 1;
SELECT 'steps', count(), sum(cityHash64(toString((arr_arr, s, dec, m)))) FROM t_filtered_deserialization PREWHERE id % 5 = 0 AND u8 % 7 = 1 AND length(arr_str) = 2
SETTINGS filtered_deserialization_max_ratio = 1, enable_multiple_prewhere_read_steps = 1;

-- PREWHERE and WHERE, primary key ranges and blocks that end inside granules.
SELECT 'ranges', count(), sum(cityHash64(toString((arr, arr_nullable, tup, ip6, uuid, fs))))
FROM t_filtered_deserialization PREWHERE i32 % 11 = 0 WHERE (id BETWEEN 1000 AND 3000 OR id BETWEEN 7000 AND 7100 OR id > 19900) AND length(arr) > 1
SETTINGS filtered_deserialization_max_ratio = 0, max_block_size = 100;
SELECT 'ranges', count(), sum(cityHash64(toString((arr, arr_nullable, tup, ip6, uuid, fs))))
FROM t_filtered_deserialization PREWHERE i32 % 11 = 0 WHERE (id BETWEEN 1000 AND 3000 OR id BETWEEN 7000 AND 7100 OR id > 19900) AND length(arr) > 1
SETTINGS filtered_deserialization_max_ratio = 1, max_block_size = 100;

-- Column that is used both in PREWHERE and read after it.
SELECT 'prewhere column', count(), sum(cityHash64(toString((arr, s)))) FROM t_filtered_deserialization PREWHERE has(arr, 5) AND id % 3 = 0 SETTINGS filtered_deserialization_max_ratio = 0;
SELECT 'prewhere column', count(), sum(cityHash64(toString((arr, s)))) FROM t_filtered_deserialization PREWHERE has(arr, 5) AND id % 3 = 0 SETTINGS filtered_deserialization_max_ratio = 1;

-- The columns cache holds whole granules, the columns read through it are not filtered.
SELECT 'columns cache', count(), sum(cityHash64(toString((arr, arr_arr, m, n.x, s))))
FROM t_filtered_deserialization PREWHERE id % 31 = 5 SETTINGS filtered_deserialization_max_ratio = 0, use_columns_cache = 1;
SELECT 'columns cache', count(), sum(cityHash64(toString((arr, arr_arr, m, n.x, s))))
FROM t_filtered_deserialization PREWHERE id % 31 = 5 SETTINGS filtered_deserialization_max_ratio = 1, use_columns_cache = 1;

-- An element of `Nested` added after the data was written, its sizes are read from the other elements.
ALTER TABLE t_filtered_deserialization ADD COLUMN `n.z` Array(UInt64);
SELECT 'nested added', count(), sum(cityHash64(toString((n.z, n.x, s)))) FROM t_filtered_deserialization PREWHERE id % 31 = 5 SETTINGS filtered_deserialization_max_ratio = 0;
SELECT 'nested added', count(), sum(cityHash64(toString((n.z, n.x, s)))) FROM t_filtered_deserialization PREWHERE id % 31 = 5 SETTINGS filtered_deserialization_max_ratio = 1;
SELECT 'nested added alone', count(), sum(cityHash64(toString(n.z))) FROM t_filtered_deserialization PREWHERE id % 31 = 5 SETTINGS filtered_deserialization_max_ratio = 0;
SELECT 'nested added alone', count(), sum(cityHash64(toString(n.z))) FROM t_filtered_deserialization PREWHERE id % 31 = 5 SETTINGS filtered_deserialization_max_ratio = 1;

DROP TABLE t_filtered_deserialization;

-- Strings with a single stream and compact parts.
DROP TABLE IF EXISTS t_filtered_deserialization_single_stream;
CREATE TABLE t_filtered_deserialization_single_stream (id UInt64, s String, arr Array(String), t Tuple(String, Array(String)))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, min_bytes_for_wide_part = 0, string_serialization_version = 'single_stream';
INSERT INTO t_filtered_deserialization_single_stream SELECT number, toString(number), arrayMap(x -> toString(x), range(number % 5)), (repeat('a', number % 10), [toString(number)]) FROM numbers(10000);
SELECT 'single stream', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization_single_stream PREWHERE id % 41 = 7 SETTINGS filtered_deserialization_max_ratio = 0;
SELECT 'single stream', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization_single_stream PREWHERE id % 41 = 7 SETTINGS filtered_deserialization_max_ratio = 1;
DROP TABLE t_filtered_deserialization_single_stream;

DROP TABLE IF EXISTS t_filtered_deserialization_compact;
CREATE TABLE t_filtered_deserialization_compact (id UInt64, s String, arr Array(UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, min_bytes_for_wide_part = '1Gi', min_rows_for_wide_part = 1000000000;
INSERT INTO t_filtered_deserialization_compact SELECT number, toString(number), range(number % 5) FROM numbers(10000);
SELECT 'compact', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization_compact PREWHERE id % 41 = 7 SETTINGS filtered_deserialization_max_ratio = 0;
SELECT 'compact', count(), sum(cityHash64(toString(tuple(*)))) FROM t_filtered_deserialization_compact PREWHERE id % 41 = 7 SETTINGS filtered_deserialization_max_ratio = 1, log_comment = '05322_compact';
DROP TABLE t_filtered_deserialization_compact;

-- The limits on read bytes count the skipped rows.
DROP TABLE IF EXISTS t_filtered_deserialization_bytes;
CREATE TABLE t_filtered_deserialization_bytes (id UInt64, s String, arr Array(UInt64))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_filtered_deserialization_bytes SELECT number, repeat('a', 1000), range(100) FROM numbers(10000);
SELECT count() FROM t_filtered_deserialization_bytes PREWHERE id % 97 = 3 WHERE NOT ignore(s) SETTINGS filtered_deserialization_max_ratio = 1, max_bytes_to_read = 1000000, max_threads = 1; -- { serverError TOO_MANY_BYTES }
SELECT count() FROM t_filtered_deserialization_bytes PREWHERE id % 97 = 3 WHERE NOT ignore(arr) SETTINGS filtered_deserialization_max_ratio = 1, max_bytes_to_read = 1000000, max_threads = 1; -- { serverError TOO_MANY_BYTES }
DROP TABLE t_filtered_deserialization_bytes;

-- The sizes of the arrays in the passed and the skipped rows differ.
CREATE TABLE t_filtered_deserialization_bytes (id UInt64, arr Array(UInt64))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_filtered_deserialization_bytes SELECT number, if(number % 100 = 99, [], range(100)) FROM numbers(8192);
SELECT count() FROM t_filtered_deserialization_bytes PREWHERE id % 100 = 99 WHERE NOT ignore(arr) SETTINGS filtered_deserialization_max_ratio = 1, max_bytes_to_read = 1000000, max_threads = 1; -- { serverError TOO_MANY_BYTES }
TRUNCATE TABLE t_filtered_deserialization_bytes;
INSERT INTO t_filtered_deserialization_bytes SELECT number, if(number % 100 = 99, range(100), []) FROM numbers(8192);
SELECT 'large passed arrays', count() FROM t_filtered_deserialization_bytes PREWHERE id % 100 = 99 WHERE NOT ignore(arr) SETTINGS filtered_deserialization_max_ratio = 1, max_bytes_to_read = 1000000, max_threads = 1;
DROP TABLE t_filtered_deserialization_bytes;

-- The rows filtered out by PREWHERE are not deserialized from wide parts.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['RowsSkippedByFilteredDeserialization'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05322_disabled', '05322_enabled', '05322_compact') AND type = 'QueryFinish' AND event_date >= yesterday()
ORDER BY log_comment;
