-- Scalar CAST indexes cover DateTime64/Enum expressions; array-wrapper indexes cover canonical numeric array functions.

DROP TABLE IF EXISTS t_bf_enum_shared;
DROP TABLE IF EXISTS t_bf_cast_numeric;
DROP TABLE IF EXISTS t_bf_signedzero;
DROP TABLE IF EXISTS t_bf_dt64_cast_text;
DROP TABLE IF EXISTS t_bf_fixed_array_wrappers;

SET max_threads = 2;
SET max_execution_time = 10;
SET max_memory_usage = 1073741824;

CREATE TABLE t_bf_cast_numeric
(
    id UInt64,
    ts String,
    enum8_code String,
    enum16_code String,
    nums Array(UInt64),
    INDEX idx_dt64_cast CAST(ts AS DateTime64(3, 'UTC')) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_enum8_cast CAST(enum8_code AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_enum16_cast CAST(enum16_code AS Enum16('zero' = 1000, 'one' = 300, 'two' = -1000)) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_nums_numeric arrayConcat(arrayDistinct(nums), nums) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_nums_constant arrayConcat(arrayDistinct(nums), [toUInt64(999)]) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_cast_numeric VALUES
    (1, '2020-01-01 00:00:01.001', 'one', 'one', [1, 1]),
    (2, '2020-01-02 00:00:02.002', 'two', 'two', [2, 2]),
    (3, '2020-01-03 00:00:03.003', 'zero', 'zero', [103, 103]),
    (4, '2020-01-04 00:00:04.004', 'zero', 'zero', [104, 104]),
    (5, '2020-01-05 00:00:05.005', 'zero', 'zero', [105, 105]),
    (6, '2020-01-06 00:00:06.006', 'zero', 'zero', [106, 106]),
    (7, '2020-01-07 00:00:07.007', 'zero', 'zero', [107, 107]),
    (8, '2020-01-08 00:00:08.008', 'zero', 'zero', [108, 108]),
    (9, '2020-01-09 00:00:09.009', 'zero', 'zero', [109, 109]),
    (10, '2020-01-10 00:00:10.010', 'zero', 'zero', [110, 110]),
    (11, '2020-01-11 00:00:11.011', 'zero', 'zero', [111, 111]),
    (12, '2020-01-12 00:00:12.012', 'zero', 'zero', [112, 112]),
    (13, '2020-01-13 00:00:13.013', 'zero', 'zero', [113, 113]),
    (14, '2020-01-14 00:00:14.014', 'zero', 'zero', [114, 114]),
    (15, '2020-01-15 00:00:15.015', 'zero', 'zero', [115, 115]),
    (16, '2020-01-16 00:00:16.016', 'zero', 'zero', [116, 116]);

CREATE TABLE t_bf_enum_shared
(
    id UInt64,
    code Enum16(
        'bf_enum_0000000000000000' = 0, 'bf_enum_0000000000000001' = 1, 'bf_enum_0000000000000002' = 2, 'bf_enum_0000000000000003' = 3,
        'bf_enum_0000000000000004' = 4, 'bf_enum_0000000000000005' = 5, 'bf_enum_0000000000000006' = 6, 'bf_enum_0000000000000007' = 7,
        'bf_enum_0000000000000008' = 8, 'bf_enum_0000000000000009' = 9, 'bf_enum_0000000000000010' = 10, 'bf_enum_0000000000000011' = 11,
        'bf_enum_0000000000000012' = 12, 'bf_enum_0000000000000013' = 13, 'bf_enum_0000000000000014' = 14, 'bf_enum_0000000000000015' = 15,
        'bf_enum_0000000000000016' = 16, 'bf_enum_0000000000000017' = 17, 'bf_enum_0000000000000018' = 18, 'bf_enum_0000000000000019' = 19,
        'bf_enum_0000000000000020' = 20, 'bf_enum_0000000000000021' = 21, 'bf_enum_0000000000000022' = 22, 'bf_enum_0000000000000023' = 23,
        'bf_enum_0000000000000024' = 24, 'bf_enum_0000000000000025' = 25, 'bf_enum_0000000000000026' = 26, 'bf_enum_0000000000000027' = 27,
        'bf_enum_0000000000000028' = 28, 'bf_enum_0000000000000029' = 29, 'bf_enum_0000000000000030' = 30, 'bf_enum_0000000000000031' = 31,
        'bf_enum_0000000000000032' = 32, 'bf_enum_0000000000000033' = 33, 'bf_enum_0000000000000034' = 34, 'bf_enum_0000000000000035' = 35,
        'bf_enum_0000000000000036' = 36, 'bf_enum_0000000000000037' = 37, 'bf_enum_0000000000000038' = 38, 'bf_enum_0000000000000039' = 39,
        'bf_enum_0000000000000040' = 40, 'bf_enum_0000000000000041' = 41, 'bf_enum_0000000000000042' = 42, 'bf_enum_0000000000000043' = 43,
        'bf_enum_0000000000000044' = 44, 'bf_enum_0000000000000045' = 45, 'bf_enum_0000000000000046' = 46, 'bf_enum_0000000000000047' = 47,
        'bf_enum_0000000000000048' = 48, 'bf_enum_0000000000000049' = 49, 'bf_enum_0000000000000050' = 50, 'bf_enum_0000000000000051' = 51,
        'bf_enum_0000000000000052' = 52, 'bf_enum_0000000000000053' = 53, 'bf_enum_0000000000000054' = 54, 'bf_enum_0000000000000055' = 55,
        'bf_enum_0000000000000056' = 56, 'bf_enum_0000000000000057' = 57, 'bf_enum_0000000000000058' = 58, 'bf_enum_0000000000000059' = 59,
        'bf_enum_0000000000000060' = 60, 'bf_enum_0000000000000061' = 61, 'bf_enum_0000000000000062' = 62, 'bf_enum_0000000000000063' = 63,
        'bf_enum_0000000000000064' = 64, 'bf_enum_0000000000000065' = 65, 'bf_enum_0000000000000066' = 66, 'bf_enum_0000000000000067' = 67,
        'bf_enum_0000000000000068' = 68, 'bf_enum_0000000000000069' = 69, 'bf_enum_0000000000000070' = 70, 'bf_enum_0000000000000071' = 71,
        'bf_enum_0000000000000072' = 72, 'bf_enum_0000000000000073' = 73, 'bf_enum_0000000000000074' = 74, 'bf_enum_0000000000000075' = 75,
        'bf_enum_0000000000000076' = 76, 'bf_enum_0000000000000077' = 77, 'bf_enum_0000000000000078' = 78, 'bf_enum_0000000000000079' = 79,
        'bf_enum_0000000000000080' = 80, 'bf_enum_0000000000000081' = 81, 'bf_enum_0000000000000082' = 82, 'bf_enum_0000000000000083' = 83,
        'bf_enum_0000000000000084' = 84, 'bf_enum_0000000000000085' = 85, 'bf_enum_0000000000000086' = 86, 'bf_enum_0000000000000087' = 87,
        'bf_enum_0000000000000088' = 88, 'bf_enum_0000000000000089' = 89, 'bf_enum_0000000000000090' = 90, 'bf_enum_0000000000000091' = 91,
        'bf_enum_0000000000000092' = 92, 'bf_enum_0000000000000093' = 93, 'bf_enum_0000000000000094' = 94, 'bf_enum_0000000000000095' = 95
    ),
    INDEX idx_code code TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_enum_shared VALUES
    (1, 'bf_enum_0000000000000003'),
    (2, 'bf_enum_0000000000000010'),
    (3, 'bf_enum_0000000000000011'),
    (4, 'bf_enum_0000000000000012'),
    (5, 'bf_enum_0000000000000013'),
    (6, 'bf_enum_0000000000000014'),
    (7, 'bf_enum_0000000000000015'),
    (8, 'bf_enum_0000000000000016'),
    (9, 'bf_enum_0000000000000017'),
    (10, 'bf_enum_0000000000000018'),
    (11, 'bf_enum_0000000000000019'),
    (12, 'bf_enum_0000000000000020'),
    (13, 'bf_enum_0000000000000021'),
    (14, 'bf_enum_0000000000000022'),
    (15, 'bf_enum_0000000000000023'),
    (16, 'bf_enum_0000000000000024');

-- The scalar subqueries share query progress, including row estimates.
-- Check complete typed results here; check pruning with the EXPLAIN assertions below.
WITH
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(ts AS DateTime64(3, 'UTC')) AS value FROM t_bf_cast_numeric WHERE CAST(ts AS DateTime64(3, 'UTC')) IN (toDateTime64('2020-01-01 00:00:01.001', 3, 'UTC'))) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(ts AS DateTime64(3, 'UTC')) AS value FROM t_bf_cast_numeric WHERE CAST(ts AS DateTime64(3, 'UTC')) IN (toDateTime64('2020-01-01 00:00:01.001', 3, 'UTC'))) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'DateTime64(3, UTC) CAST preserves typed rows', indexed = full_scan AND indexed = [(toUInt64(1), toDateTime64('2020-01-01 00:00:01.001', 3, 'UTC'))];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_dt64_cast') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'DateTime64(3, UTC) CAST declines unknown persisted context, reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, CAST(ts AS DateTime64(3, 'UTC')) AS value FROM t_bf_cast_numeric WHERE CAST(ts AS DateTime64(3, 'UTC')) IN (toDateTime64('2020-01-01 00:00:01.001', 3, 'UTC')));

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(ts AS DateTime64(6, 'UTC')) AS value FROM t_bf_cast_numeric WHERE CAST(ts AS DateTime64(6, 'UTC')) IN (toDateTime64('2020-01-01 00:00:01.001000', 6, 'UTC'))) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(ts AS DateTime64(6, 'UTC')) AS value FROM t_bf_cast_numeric WHERE CAST(ts AS DateTime64(6, 'UTC')) IN (toDateTime64('2020-01-01 00:00:01.001000', 6, 'UTC'))) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'DateTime64 scale mismatch preserves typed rows', indexed = full_scan AND indexed = [(toUInt64(1), toDateTime64('2020-01-01 00:00:01.001000', 6, 'UTC'))];

SELECT 'DateTime64 scale mismatch declines idx_dt64_cast', max(explain LIKE '%Name: idx_dt64_cast%') = 0
FROM (EXPLAIN indexes = 1 SELECT id, CAST(ts AS DateTime64(6, 'UTC')) AS value FROM t_bf_cast_numeric WHERE CAST(ts AS DateTime64(6, 'UTC')) IN (toDateTime64('2020-01-01 00:00:01.001000', 6, 'UTC')));

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(ts AS DateTime64(3, 'Europe/Berlin')) AS value FROM t_bf_cast_numeric WHERE CAST(ts AS DateTime64(3, 'Europe/Berlin')) IN (toDateTime64('2020-01-01 00:00:01.001', 3, 'Europe/Berlin'))) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(ts AS DateTime64(3, 'Europe/Berlin')) AS value FROM t_bf_cast_numeric WHERE CAST(ts AS DateTime64(3, 'Europe/Berlin')) IN (toDateTime64('2020-01-01 00:00:01.001', 3, 'Europe/Berlin'))) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'DateTime64 timezone mismatch preserves typed rows', indexed = full_scan AND indexed = [(toUInt64(1), toDateTime64('2020-01-01 00:00:01.001', 3, 'Europe/Berlin'))];

SELECT 'DateTime64 timezone mismatch declines idx_dt64_cast', max(explain LIKE '%Name: idx_dt64_cast%') = 0
FROM (EXPLAIN indexes = 1 SELECT id, CAST(ts AS DateTime64(3, 'Europe/Berlin')) AS value FROM t_bf_cast_numeric WHERE CAST(ts AS DateTime64(3, 'Europe/Berlin')) IN (toDateTime64('2020-01-01 00:00:01.001', 3, 'Europe/Berlin')));

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(ts AS DateTime64(3)) AS value FROM t_bf_cast_numeric WHERE CAST(ts AS DateTime64(3)) IN (toDateTime64('2020-01-01 00:00:01.001', 3))) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(ts AS DateTime64(3)) AS value FROM t_bf_cast_numeric WHERE CAST(ts AS DateTime64(3)) IN (toDateTime64('2020-01-01 00:00:01.001', 3))) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'DateTime64 implicit timezone preserves typed rows', indexed = full_scan AND indexed = [(toUInt64(1), toDateTime64('2020-01-01 00:00:01.001', 3))];

SELECT 'DateTime64 implicit timezone declines explicit idx_dt64_cast', max(explain LIKE '%Name: idx_dt64_cast%') = 0
FROM (EXPLAIN indexes = 1 SELECT id, CAST(ts AS DateTime64(3)) AS value FROM t_bf_cast_numeric WHERE CAST(ts AS DateTime64(3)) IN (toDateTime64('2020-01-01 00:00:01.001', 3)));

WITH
    (SELECT arraySort(groupArray((toUInt64(id), code))) FROM t_bf_enum_shared WHERE code IN ('bf_enum_0000000000000003') SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), code))) FROM t_bf_enum_shared WHERE code IN ('bf_enum_0000000000000003') SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'Shared Enum16 raw column preserves typed rows', indexed = full_scan AND arrayMap(x -> (x.1, toString(x.2)), indexed) = [(toUInt64(1), 'bf_enum_0000000000000003')] AND position(toTypeName(indexed), 'Enum16(') > 0;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_code') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Shared Enum16 raw column prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, code FROM t_bf_enum_shared WHERE code IN ('bf_enum_0000000000000003'));

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(enum8_code AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)) AS value FROM t_bf_cast_numeric WHERE CAST(enum8_code AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)) IN (CAST('one' AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)))) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(enum8_code AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)) AS value FROM t_bf_cast_numeric WHERE CAST(enum8_code AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)) IN (CAST('one' AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)))) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'Enum8 CAST preserves typed rows', indexed = full_scan AND indexed = [(toUInt64(1), CAST('one' AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)))];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_enum8_cast') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Enum8 CAST prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, CAST(enum8_code AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)) AS value FROM t_bf_cast_numeric WHERE CAST(enum8_code AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)) IN (CAST('one' AS Enum8('zero' = 0, 'one' = 1, 'two' = 2))));

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(enum16_code AS Enum16('zero' = 1000, 'one' = 300, 'two' = -1000)) AS value FROM t_bf_cast_numeric WHERE CAST(enum16_code AS Enum16('zero' = 1000, 'one' = 300, 'two' = -1000)) IN (CAST('one' AS Enum16('zero' = 1000, 'one' = 300, 'two' = -1000)))) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(enum16_code AS Enum16('zero' = 1000, 'one' = 300, 'two' = -1000)) AS value FROM t_bf_cast_numeric WHERE CAST(enum16_code AS Enum16('zero' = 1000, 'one' = 300, 'two' = -1000)) IN (CAST('one' AS Enum16('zero' = 1000, 'one' = 300, 'two' = -1000)))) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'Enum16 CAST preserves typed rows', indexed = full_scan AND indexed = [(toUInt64(1), CAST('one' AS Enum16('zero' = 1000, 'one' = 300, 'two' = -1000)))];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_enum16_cast') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Enum16 CAST prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, CAST(enum16_code AS Enum16('zero' = 1000, 'one' = 300, 'two' = -1000)) AS value FROM t_bf_cast_numeric WHERE CAST(enum16_code AS Enum16('zero' = 1000, 'one' = 300, 'two' = -1000)) IN (CAST('one' AS Enum16('zero' = 1000, 'one' = 300, 'two' = -1000))));

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(enum8_code AS Enum8('zero' = 0, 'one' = 2, 'two' = 1)) AS value FROM t_bf_cast_numeric WHERE CAST(enum8_code AS Enum8('zero' = 0, 'one' = 2, 'two' = 1)) IN (CAST('one' AS Enum8('zero' = 0, 'one' = 2, 'two' = 1)))) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(enum8_code AS Enum8('zero' = 0, 'one' = 2, 'two' = 1)) AS value FROM t_bf_cast_numeric WHERE CAST(enum8_code AS Enum8('zero' = 0, 'one' = 2, 'two' = 1)) IN (CAST('one' AS Enum8('zero' = 0, 'one' = 2, 'two' = 1)))) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'Enum8 map mismatch preserves typed rows', indexed = full_scan AND indexed = [(toUInt64(1), CAST('one' AS Enum8('zero' = 0, 'one' = 2, 'two' = 1)))];

SELECT 'Enum8 map mismatch declines idx_enum8_cast', max(explain LIKE '%Name: idx_enum8_cast%') = 0
FROM (EXPLAIN indexes = 1 SELECT id, CAST(enum8_code AS Enum8('zero' = 0, 'one' = 2, 'two' = 1)) AS value FROM t_bf_cast_numeric WHERE CAST(enum8_code AS Enum8('zero' = 0, 'one' = 2, 'two' = 1)) IN (CAST('one' AS Enum8('zero' = 0, 'one' = 2, 'two' = 1))));

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(enum8_code AS Enum16('zero' = 0, 'one' = 1, 'two' = 2)) AS value FROM t_bf_cast_numeric WHERE CAST(enum8_code AS Enum16('zero' = 0, 'one' = 1, 'two' = 2)) IN (CAST('one' AS Enum16('zero' = 0, 'one' = 1, 'two' = 2)))) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, CAST(enum8_code AS Enum16('zero' = 0, 'one' = 1, 'two' = 2)) AS value FROM t_bf_cast_numeric WHERE CAST(enum8_code AS Enum16('zero' = 0, 'one' = 1, 'two' = 2)) IN (CAST('one' AS Enum16('zero' = 0, 'one' = 1, 'two' = 2)))) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'Enum8 index to Enum16 query preserves typed rows', indexed = full_scan AND indexed = [(toUInt64(1), CAST('one' AS Enum16('zero' = 0, 'one' = 1, 'two' = 2)))];

SELECT 'Enum8 index to Enum16 query declines idx_enum8_cast', max(explain LIKE '%Name: idx_enum8_cast%') = 0
FROM (EXPLAIN indexes = 1 SELECT id, CAST(enum8_code AS Enum16('zero' = 0, 'one' = 1, 'two' = 2)) AS value FROM t_bf_cast_numeric WHERE CAST(enum8_code AS Enum16('zero' = 0, 'one' = 1, 'two' = 2)) IN (CAST('one' AS Enum16('zero' = 0, 'one' = 1, 'two' = 2))));

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, value FROM t_bf_cast_numeric ARRAY JOIN arrayConcat(arrayDistinct(nums), nums) AS value WHERE value IN (toUInt64(103))) SETTINGS use_skip_indexes = 0, max_rows_to_read = 64) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, value FROM t_bf_cast_numeric ARRAY JOIN arrayConcat(arrayDistinct(nums), nums) AS value WHERE value IN (toUInt64(103))) SETTINGS use_skip_indexes = 1, max_rows_to_read = 64) AS indexed
SELECT 'UInt64 arrayDistinct plus arrayConcat preserves duplicate rows', indexed = full_scan AND indexed = [(toUInt64(3), toUInt64(103)), (toUInt64(3), toUInt64(103)), (toUInt64(3), toUInt64(103))];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_nums_numeric') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'UInt64 arrayDistinct plus safe arrayConcat prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, value FROM t_bf_cast_numeric ARRAY JOIN arrayConcat(arrayDistinct(nums), nums) AS value WHERE value IN (toUInt64(103)) SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1);

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, value FROM t_bf_cast_numeric ARRAY JOIN CAST(arrayConcat(arrayDistinct(nums), nums), 'Array(Int64)') AS value WHERE value IN (toInt64(103))) SETTINGS use_skip_indexes = 0, max_rows_to_read = 64) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM (SELECT id, value FROM t_bf_cast_numeric ARRAY JOIN CAST(arrayConcat(arrayDistinct(nums), nums), 'Array(Int64)') AS value WHERE value IN (toInt64(103))) SETTINGS use_skip_indexes = 1, max_rows_to_read = 64) AS indexed
SELECT 'UInt64 to Int64 array coercion preserves rows', indexed = full_scan AND indexed = [(toUInt64(3), toInt64(103)), (toUInt64(3), toInt64(103)), (toUInt64(3), toInt64(103))];

SELECT 'UInt64 to Int64 array coercion declines idx_nums_numeric', max(explain LIKE '%Name: idx_nums_numeric%') = 0
FROM (EXPLAIN indexes = 1 SELECT id, value FROM t_bf_cast_numeric ARRAY JOIN CAST(arrayConcat(arrayDistinct(nums), nums), 'Array(Int64)') AS value WHERE value IN (toInt64(103)) SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1);

WITH
    (SELECT arraySort(groupArray((toUInt64(id), assumeNotNull(value)))) FROM (SELECT id, value FROM t_bf_cast_numeric ARRAY JOIN CAST(arrayConcat(arrayDistinct(nums), nums), 'Array(Nullable(UInt64))') AS value WHERE value = CAST(toUInt64(103), 'Nullable(UInt64)')) SETTINGS use_skip_indexes = 0, max_rows_to_read = 64) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), assumeNotNull(value)))) FROM (SELECT id, value FROM t_bf_cast_numeric ARRAY JOIN CAST(arrayConcat(arrayDistinct(nums), nums), 'Array(Nullable(UInt64))') AS value WHERE value = CAST(toUInt64(103), 'Nullable(UInt64)')) SETTINGS use_skip_indexes = 1, max_rows_to_read = 64) AS indexed
SELECT 'UInt64 nullable array wrapper preserves rows', indexed = full_scan AND indexed = [(toUInt64(3), toUInt64(103)), (toUInt64(3), toUInt64(103)), (toUInt64(3), toUInt64(103))];

SELECT 'UInt64 nullable array wrapper declines idx_nums_numeric', max(explain LIKE '%Name: idx_nums_numeric%') = 0
FROM (EXPLAIN indexes = 1 SELECT id, value FROM t_bf_cast_numeric ARRAY JOIN CAST(arrayConcat(arrayDistinct(nums), nums), 'Array(Nullable(UInt64))') AS value WHERE value = CAST(toUInt64(103), 'Nullable(UInt64)') SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1);

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value)))
     FROM t_bf_cast_numeric ARRAY JOIN arrayConcat(arrayDistinct(nums), [toUInt64(999)]) AS value
     WHERE value IN (toUInt64(103))
     SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
              query_plan_fuse_filter_into_array_join = 1, use_skip_indexes = 0, max_rows_to_read = 64) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value)))
     FROM t_bf_cast_numeric ARRAY JOIN arrayConcat(arrayDistinct(nums), [toUInt64(999)]) AS value
     WHERE value IN (toUInt64(103))
     SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
              query_plan_fuse_filter_into_array_join = 1, use_skip_indexes = 1, max_rows_to_read = 64) AS indexed
SELECT 'UInt64 numeric constant append selective needle preserves typed rows',
       indexed = full_scan AND full_scan = [(toUInt64(3), toUInt64(103))];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_nums_constant') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'UInt64 numeric constant append selective needle declines named index, reads 16/16',
       name_row = 0 AND primary_key_row > 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, value FROM t_bf_cast_numeric
      ARRAY JOIN arrayConcat(arrayDistinct(nums), [toUInt64(999)]) AS value
      WHERE value IN (toUInt64(103))
      SETTINGS query_plan_lower_array_join_function = 1,
               query_plan_enable_optimizations = 1,
               query_plan_fuse_filter_into_array_join = 1,
               max_rows_to_read = 16);

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value)))
     FROM t_bf_cast_numeric ARRAY JOIN arrayConcat(arrayDistinct(nums), [toUInt64(1000)]) AS value
     WHERE value IN (toUInt64(1000))
     SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
              query_plan_fuse_filter_into_array_join = 1, use_skip_indexes = 0, max_rows_to_read = 64) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value)))
     FROM t_bf_cast_numeric ARRAY JOIN arrayConcat(arrayDistinct(nums), [toUInt64(1000)]) AS value
     WHERE value IN (toUInt64(1000))
     SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
              query_plan_fuse_filter_into_array_join = 1, use_skip_indexes = 1, max_rows_to_read = 64) AS indexed
SELECT 'UInt64 numeric constant mismatch preserves typed rows',
       indexed = full_scan AND full_scan = arrayMap(x -> (x, toUInt64(1000)), range(1, 17));

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_nums_constant') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'UInt64 numeric constant mismatch declines idx_nums_constant, reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, value FROM t_bf_cast_numeric
      ARRAY JOIN arrayConcat(arrayDistinct(nums), [toUInt64(1000)]) AS value
      WHERE value IN (toUInt64(1000))
      SETTINGS query_plan_lower_array_join_function = 1,
               query_plan_enable_optimizations = 1,
               query_plan_fuse_filter_into_array_join = 1,
               max_rows_to_read = 16);

CREATE TABLE t_bf_signedzero
(
    id UInt64,
    f Float64,
    INDEX idx_signedzero reinterpretAsUInt64(f + toFloat64(0.0)) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_signedzero VALUES (1, toFloat64(-0.0)), (2, toFloat64(0.0)), (3, toFloat64(1.0));

WITH
    (SELECT arraySort(groupArray(toUInt64(id))) FROM t_bf_signedzero WHERE reinterpretAsUInt64(f + toFloat64(-0.0)) IN (toUInt64(9223372036854775808)) SETTINGS use_skip_indexes = 0, max_rows_to_read = 8) AS full_scan,
    (SELECT arraySort(groupArray(toUInt64(id))) FROM t_bf_signedzero WHERE reinterpretAsUInt64(f + toFloat64(-0.0)) IN (toUInt64(9223372036854775808)) SETTINGS use_skip_indexes = 1, max_rows_to_read = 8) AS indexed
SELECT 'signedzero expression returns UInt64 and preserves matching row', indexed = full_scan AND indexed = [toUInt64(1)];

SELECT 'signedzero bit-mismatched constant declines idx_signedzero', max(explain LIKE '%Name: idx_signedzero%') = 0
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_signedzero WHERE reinterpretAsUInt64(f + toFloat64(-0.0)) IN (toUInt64(9223372036854775808)));

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_dt64_cast') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'DateTime64 CAST format flag cannot prove persisted context, reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, CAST(ts AS DateTime64(3, 'UTC')) AS value FROM t_bf_cast_numeric
      WHERE CAST(ts AS DateTime64(3, 'UTC')) IN (toDateTime64('2020-01-01 00:00:01.001', 3, 'UTC'))
      SETTINGS input_format_parquet_dictionary_filter_push_down = 0, use_skip_indexes = 1, max_rows_to_read = 16);

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM
        (SELECT id, CAST(ts AS DateTime64(3, 'UTC')) AS value FROM t_bf_cast_numeric
         WHERE CAST(ts AS DateTime64(3, 'UTC')) IN (toDateTime64('2020-01-01 00:00:01.001', 3, 'UTC')))
     SETTINGS input_format_parquet_dictionary_filter_push_down = 0, use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM
        (SELECT id, CAST(ts AS DateTime64(3, 'UTC')) AS value FROM t_bf_cast_numeric
         WHERE CAST(ts AS DateTime64(3, 'UTC')) IN (toDateTime64('2020-01-01 00:00:01.001', 3, 'UTC')))
     SETTINGS input_format_parquet_dictionary_filter_push_down = 0, use_skip_indexes = 1) AS indexed
SELECT 'DateTime64 CAST parquet dictionary flag 0 full OFF/ON typed rows',
       indexed = full_scan AND full_scan = [(toUInt64(1), toDateTime64('2020-01-01 00:00:01.001', 3, 'UTC'))];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_enum8_cast') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Enum8 CAST parallel-parsing flag 0 keeps named index at 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, CAST(enum8_code AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)) AS value FROM t_bf_cast_numeric
      WHERE CAST(enum8_code AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)) IN (CAST('one' AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)))
      SETTINGS input_format_parallel_parsing = 0, use_skip_indexes = 1, max_rows_to_read = 16);

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM
        (SELECT id, CAST(enum8_code AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)) AS value FROM t_bf_cast_numeric
         WHERE CAST(enum8_code AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)) IN (CAST('one' AS Enum8('zero' = 0, 'one' = 1, 'two' = 2))))
     SETTINGS input_format_parallel_parsing = 0, use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM
        (SELECT id, CAST(enum8_code AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)) AS value FROM t_bf_cast_numeric
         WHERE CAST(enum8_code AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)) IN (CAST('one' AS Enum8('zero' = 0, 'one' = 1, 'two' = 2))))
     SETTINGS input_format_parallel_parsing = 0, use_skip_indexes = 1) AS indexed
SELECT 'Enum8 CAST parallel-parsing flag 0 full OFF/ON typed rows',
       indexed = full_scan AND full_scan = [(toUInt64(1), CAST('one' AS Enum8('zero' = 0, 'one' = 1, 'two' = 2)))];

CREATE TABLE t_bf_dt64_cast_text
(
    id UInt64,
    ts DateTime64(7, 'UTC'),
    INDEX idx_dt64_cast_text CAST(ts AS String) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_dt64_cast_text VALUES
    (1, toDateTime64('2020-01-01 00:00:01.1234567', 7, 'UTC')),
    (2, toDateTime64('2020-01-02 00:00:02.0000001', 7, 'UTC')),
    (3, toDateTime64('2020-01-03 00:00:03.0000002', 7, 'UTC')),
    (4, toDateTime64('2020-01-04 00:00:04.0000003', 7, 'UTC')),
    (5, toDateTime64('2020-01-05 00:00:05.0000004', 7, 'UTC')),
    (6, toDateTime64('2020-01-06 00:00:06.0000005', 7, 'UTC')),
    (7, toDateTime64('2020-01-07 00:00:07.0000006', 7, 'UTC')),
    (8, toDateTime64('2020-01-08 00:00:08.0000007', 7, 'UTC')),
    (9, toDateTime64('2020-01-09 00:00:09.0000008', 7, 'UTC')),
    (10, toDateTime64('2020-01-10 00:00:10.0000009', 7, 'UTC')),
    (11, toDateTime64('2020-01-11 00:00:11.0000010', 7, 'UTC')),
    (12, toDateTime64('2020-01-12 00:00:12.0000011', 7, 'UTC')),
    (13, toDateTime64('2020-01-13 00:00:13.0000012', 7, 'UTC')),
    (14, toDateTime64('2020-01-14 00:00:14.0000013', 7, 'UTC')),
    (15, toDateTime64('2020-01-15 00:00:15.0000014', 7, 'UTC')),
    (16, toDateTime64('2020-01-16 00:00:16.0000015', 7, 'UTC'));

WITH
    (SELECT arraySort(groupArray((toUInt64(id), CAST(ts AS String)))) FROM t_bf_dt64_cast_text
     WHERE CAST(ts AS String) IN ('2020-01-01 00:00:01.1234567')
     SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), CAST(ts AS String)))) FROM t_bf_dt64_cast_text
     WHERE CAST(ts AS String) IN ('2020-01-01 00:00:01.1234567')
     SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'DateTime64(7) CAST-to-String default context preserves typed rows',
       indexed = full_scan AND full_scan = [(toUInt64(1), '2020-01-01 00:00:01.1234567')];

-- DateTime64-to-String is outside the closed context-independent CAST subset. Declining the index must preserve rows.
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_dt64_cast_text') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'DateTime64(7) CAST-to-String format-only mismatch declines index, reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, CAST(ts AS String) AS value FROM t_bf_dt64_cast_text
      WHERE CAST(ts AS String) IN ('2020-01-01 00:00:01.1234567')
      SETTINGS output_format_parallel_formatting = 0, use_skip_indexes = 1, max_rows_to_read = 16);

WITH
    (SELECT arraySort(groupArray((toUInt64(id), CAST(ts AS String)))) FROM t_bf_dt64_cast_text
     WHERE CAST(ts AS String) IN ('2020-01-01 00:00:01.1234567')
     SETTINGS output_format_parallel_formatting = 0, use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), CAST(ts AS String)))) FROM t_bf_dt64_cast_text
     WHERE CAST(ts AS String) IN ('2020-01-01 00:00:01.1234567')
     SETTINGS output_format_parallel_formatting = 0, use_skip_indexes = 1) AS indexed
SELECT 'DateTime64(7) CAST-to-String format-only mismatch full OFF/ON typed rows',
       indexed = full_scan AND full_scan = [(toUInt64(1), '2020-01-01 00:00:01.1234567')];

WITH
    (SELECT arraySort(groupArray((toUInt64(id), CAST(ts AS String)))) FROM t_bf_dt64_cast_text
     WHERE CAST(ts AS String) IN ('2020-01-01 00:00:01.1234567')
     SETTINGS allow_special_serialization_kinds_in_output_formats = 0,
              input_format_parquet_dictionary_filter_push_down = 0,
              output_format_parallel_formatting = 0,
              input_format_parallel_parsing = 0,
              use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), CAST(ts AS String)))) FROM t_bf_dt64_cast_text
     WHERE CAST(ts AS String) IN ('2020-01-01 00:00:01.1234567')
     SETTINGS allow_special_serialization_kinds_in_output_formats = 0,
              input_format_parquet_dictionary_filter_push_down = 0,
              output_format_parallel_formatting = 0,
              input_format_parallel_parsing = 0,
              use_skip_indexes = 1) AS indexed
SELECT 'DateTime64(7) CAST-to-String four format flags full OFF/ON typed rows',
       indexed = full_scan AND full_scan = [(toUInt64(1), '2020-01-01 00:00:01.1234567')];

WITH
    (SELECT arraySort(groupArray((toUInt64(id), CAST(ts AS String)))) FROM t_bf_dt64_cast_text
     WHERE CAST(ts AS String) IN ('2020-01-01 00:00:01.123456700')
     SETTINGS date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1,
              use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), CAST(ts AS String)))) FROM t_bf_dt64_cast_text
     WHERE CAST(ts AS String) IN ('2020-01-01 00:00:01.123456700')
     SETTINGS date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1,
              use_skip_indexes = 1) AS indexed
SELECT 'DateTime64(7) trailing-zero mismatch full OFF/ON typed rows',
       indexed = full_scan AND full_scan = [(toUInt64(1), '2020-01-01 00:00:01.123456700')];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_dt64_cast_text') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'DateTime64(7) trailing-zero mismatch declines idx_dt64_cast_text, reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, CAST(ts AS String) AS value FROM t_bf_dt64_cast_text
      WHERE CAST(ts AS String) IN ('2020-01-01 00:00:01.123456700')
      SETTINGS date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1,
               use_skip_indexes = 1, max_rows_to_read = 16);

CREATE TABLE t_bf_fixed_array_wrappers
(
    id UInt64,
    dt_values Array(DateTime64(3, 'UTC')),
    uuid_values Array(UUID),
    fixed_values Array(FixedString(4)),
    INDEX idx_fixed_dt64_values arrayConcat(arrayDistinct(dt_values), [toDateTime64('2020-01-01 00:00:00.999', 3, 'UTC')]) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_fixed_uuid_values arrayConcat(arrayDistinct(uuid_values), [toUUID('00000000-0000-0000-0000-000000000999')]) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_fixed_string_values arrayConcat(arrayDistinct(fixed_values), [toFixedString('Z', 4)]) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_fixed_array_wrappers
SELECT toUInt64(number + 1),
       if(number = 2,
          [toDateTime64('2020-01-01 00:00:00.103', 3, 'UTC'), toDateTime64('2020-01-01 00:00:00.103', 3, 'UTC')],
          [toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC'), toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC')]),
       if(number = 2,
          [toUUID('00000000-0000-0000-0000-000000000103'), toUUID('00000000-0000-0000-0000-000000000103')],
          [toUUID('00000000-0000-0000-0000-000000000000'), toUUID('00000000-0000-0000-0000-000000000000')]),
       if(number = 2,
          [toFixedString('A', 4), toFixedString('A', 4)],
          [toFixedString('B', 4), toFixedString('B', 4)])
FROM numbers(16);

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM t_bf_fixed_array_wrappers
     ARRAY JOIN arrayConcat(arrayDistinct(dt_values), [toDateTime64('2020-01-01 00:00:00.999', 3, 'UTC')]) AS value
     WHERE value IN (toDateTime64('2020-01-01 00:00:00.103', 3, 'UTC'))
     SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
              query_plan_fuse_filter_into_array_join = 1, use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM t_bf_fixed_array_wrappers
     ARRAY JOIN arrayConcat(arrayDistinct(dt_values), [toDateTime64('2020-01-01 00:00:00.999', 3, 'UTC')]) AS value
     WHERE value IN (toDateTime64('2020-01-01 00:00:00.103', 3, 'UTC'))
     SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
              query_plan_fuse_filter_into_array_join = 1, use_skip_indexes = 1) AS indexed
SELECT 'DateTime64 array wrapper selective needle preserves typed rows',
       indexed = full_scan AND full_scan = [(toUInt64(3), toDateTime64('2020-01-01 00:00:00.103', 3, 'UTC'))];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_fixed_dt64_values') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'DateTime64 array wrapper selective needle declines named index, reads 16/16',
       name_row = 0 AND primary_key_row > 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, value FROM t_bf_fixed_array_wrappers
      ARRAY JOIN arrayConcat(arrayDistinct(dt_values), [toDateTime64('2020-01-01 00:00:00.999', 3, 'UTC')]) AS value
      WHERE value IN (toDateTime64('2020-01-01 00:00:00.103', 3, 'UTC'))
      SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
               query_plan_fuse_filter_into_array_join = 1, max_rows_to_read = 16);

SELECT 'DateTime64 array wrapper mismatch full scan preserves complete typed rows',
       arraySort(groupArray((toUInt64(id), value)))
           = arrayMap(x -> (x, toDateTime64('2020-01-01 00:00:00.998', 3, 'UTC')), range(1, 17))
FROM t_bf_fixed_array_wrappers
ARRAY JOIN arrayConcat(arrayDistinct(dt_values), [toDateTime64('2020-01-01 00:00:00.998', 3, 'UTC')]) AS value
WHERE value IN (toDateTime64('2020-01-01 00:00:00.998', 3, 'UTC'))
SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
         query_plan_fuse_filter_into_array_join = 1, use_skip_indexes = 0, max_rows_to_read = 32;

SELECT 'DateTime64 array wrapper mismatch indexed read preserves complete typed rows',
       arraySort(groupArray((toUInt64(id), value)))
           = arrayMap(x -> (x, toDateTime64('2020-01-01 00:00:00.998', 3, 'UTC')), range(1, 17))
FROM t_bf_fixed_array_wrappers
ARRAY JOIN arrayConcat(arrayDistinct(dt_values), [toDateTime64('2020-01-01 00:00:00.998', 3, 'UTC')]) AS value
WHERE value IN (toDateTime64('2020-01-01 00:00:00.998', 3, 'UTC'))
SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
         query_plan_fuse_filter_into_array_join = 1, use_skip_indexes = 1, max_rows_to_read = 32;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_fixed_dt64_values') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'DateTime64 array wrapper mismatch declines named index, reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, value FROM t_bf_fixed_array_wrappers
      ARRAY JOIN arrayConcat(arrayDistinct(dt_values), [toDateTime64('2020-01-01 00:00:00.998', 3, 'UTC')]) AS value
      WHERE value IN (toDateTime64('2020-01-01 00:00:00.998', 3, 'UTC'))
      SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
               query_plan_fuse_filter_into_array_join = 1, max_rows_to_read = 16);

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM t_bf_fixed_array_wrappers
     ARRAY JOIN arrayConcat(arrayDistinct(uuid_values), [toUUID('00000000-0000-0000-0000-000000000999')]) AS value
     WHERE value IN (toUUID('00000000-0000-0000-0000-000000000103'))
     SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
              query_plan_fuse_filter_into_array_join = 1, use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value))) FROM t_bf_fixed_array_wrappers
     ARRAY JOIN arrayConcat(arrayDistinct(uuid_values), [toUUID('00000000-0000-0000-0000-000000000999')]) AS value
     WHERE value IN (toUUID('00000000-0000-0000-0000-000000000103'))
     SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
              query_plan_fuse_filter_into_array_join = 1, use_skip_indexes = 1) AS indexed
SELECT 'UUID array wrapper selective needle preserves typed rows',
       indexed = full_scan AND full_scan = [(toUInt64(3), toUUID('00000000-0000-0000-0000-000000000103'))];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_fixed_uuid_values') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'UUID array wrapper selective needle declines named index, reads 16/16',
       name_row = 0 AND primary_key_row > 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, value FROM t_bf_fixed_array_wrappers
      ARRAY JOIN arrayConcat(arrayDistinct(uuid_values), [toUUID('00000000-0000-0000-0000-000000000999')]) AS value
      WHERE value IN (toUUID('00000000-0000-0000-0000-000000000103'))
      SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
               query_plan_fuse_filter_into_array_join = 1, max_rows_to_read = 16);

SELECT 'UUID array wrapper mismatch full scan preserves complete typed rows',
       arraySort(groupArray((toUInt64(id), value)))
           = arrayMap(x -> (x, toUUID('00000000-0000-0000-0000-000000000998')), range(1, 17))
FROM t_bf_fixed_array_wrappers
ARRAY JOIN arrayConcat(arrayDistinct(uuid_values), [toUUID('00000000-0000-0000-0000-000000000998')]) AS value
WHERE value IN (toUUID('00000000-0000-0000-0000-000000000998'))
SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
         query_plan_fuse_filter_into_array_join = 1, use_skip_indexes = 0, max_rows_to_read = 32;

SELECT 'UUID array wrapper mismatch indexed read preserves complete typed rows',
       arraySort(groupArray((toUInt64(id), value)))
           = arrayMap(x -> (x, toUUID('00000000-0000-0000-0000-000000000998')), range(1, 17))
FROM t_bf_fixed_array_wrappers
ARRAY JOIN arrayConcat(arrayDistinct(uuid_values), [toUUID('00000000-0000-0000-0000-000000000998')]) AS value
WHERE value IN (toUUID('00000000-0000-0000-0000-000000000998'))
SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
         query_plan_fuse_filter_into_array_join = 1, use_skip_indexes = 1, max_rows_to_read = 32;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_fixed_uuid_values') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'UUID array wrapper mismatch declines named index, reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id, value FROM t_bf_fixed_array_wrappers
      ARRAY JOIN arrayConcat(arrayDistinct(uuid_values), [toUUID('00000000-0000-0000-0000-000000000998')]) AS value
      WHERE value IN (toUUID('00000000-0000-0000-0000-000000000998'))
      SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
               query_plan_fuse_filter_into_array_join = 1, max_rows_to_read = 16);

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value, hex(value), toTypeName(value))))
     FROM t_bf_fixed_array_wrappers
     ARRAY JOIN arrayConcat(arrayDistinct(fixed_values), [toFixedString('Z', 4)]) AS value
     WHERE value IN (toFixedString('A', 4))
     SETTINGS query_plan_lower_array_join_function = 1,
              query_plan_enable_optimizations = 1,
              query_plan_fuse_filter_into_array_join = 1,
              use_skip_indexes = 0,
              max_rows_to_read = 128) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value, hex(value), toTypeName(value))))
     FROM t_bf_fixed_array_wrappers
     ARRAY JOIN arrayConcat(arrayDistinct(fixed_values), [toFixedString('Z', 4)]) AS value
     WHERE value IN (toFixedString('A', 4))
     SETTINGS query_plan_lower_array_join_function = 1,
              query_plan_enable_optimizations = 1,
              query_plan_fuse_filter_into_array_join = 1,
              use_skip_indexes = 1,
              max_rows_to_read = 128) AS indexed
SELECT 'FixedString(4) constant append preserves typed padded rows',
       toUInt8(indexed = full_scan
       AND full_scan = [(toUInt64(3), toFixedString('A', 4), '41000000', 'FixedString(4)')]);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_fixed_string_values') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1, 128)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'FixedString(4) constant append declines named index, reads 16/16',
       toUInt8(name_row = 0 AND primary_key_row > 0 AND selected = 16 AND total = 16)
FROM (EXPLAIN indexes = 1
      SELECT id, value
      FROM t_bf_fixed_array_wrappers
      ARRAY JOIN arrayConcat(arrayDistinct(fixed_values), [toFixedString('Z', 4)]) AS value
      WHERE value IN (toFixedString('A', 4))
      SETTINGS query_plan_lower_array_join_function = 1,
               query_plan_enable_optimizations = 1,
               query_plan_fuse_filter_into_array_join = 1,
               max_rows_to_read = 128);

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value, hex(value), toTypeName(value))))
     FROM t_bf_fixed_array_wrappers
     ARRAY JOIN arrayConcat(arrayDistinct(fixed_values), [toFixedString('Y', 4)]) AS value
     WHERE value IN (toFixedString('Y', 4))
     SETTINGS query_plan_lower_array_join_function = 1,
              query_plan_enable_optimizations = 1,
              query_plan_fuse_filter_into_array_join = 1,
              use_skip_indexes = 0,
              max_rows_to_read = 128) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value, hex(value), toTypeName(value))))
     FROM t_bf_fixed_array_wrappers
     ARRAY JOIN arrayConcat(arrayDistinct(fixed_values), [toFixedString('Y', 4)]) AS value
     WHERE value IN (toFixedString('Y', 4))
     SETTINGS query_plan_lower_array_join_function = 1,
              query_plan_enable_optimizations = 1,
              query_plan_fuse_filter_into_array_join = 1,
              use_skip_indexes = 1,
              max_rows_to_read = 128) AS indexed
SELECT 'FixedString(4) unequal-byte append preserves all typed rows',
       toUInt8(indexed = full_scan
       AND full_scan = arrayMap(x -> (toUInt64(x), toFixedString('Y', 4), '59000000', 'FixedString(4)'), range(1, 17)));

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_fixed_string_values') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1, 128)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'FixedString(4) unequal-byte append declines index and reads 16/16',
       toUInt8(name_row = 0 AND primary_key_row > 0 AND selected = 16 AND total = 16)
FROM (EXPLAIN indexes = 1
      SELECT id, value
      FROM t_bf_fixed_array_wrappers
      ARRAY JOIN arrayConcat(arrayDistinct(fixed_values), [toFixedString('Y', 4)]) AS value
      WHERE value IN (toFixedString('Y', 4))
      SETTINGS query_plan_lower_array_join_function = 1,
               query_plan_enable_optimizations = 1,
               query_plan_fuse_filter_into_array_join = 1,
               max_rows_to_read = 128);

WITH
    (SELECT arraySort(groupArray((toUInt64(id), value, hex(value), toTypeName(value))))
     FROM t_bf_fixed_array_wrappers
     ARRAY JOIN arrayConcat(arrayDistinct(fixed_values), [toFixedString(unhex('5A0051'), 4)]) AS value
     WHERE value IN (toFixedString(unhex('5A0051'), 4))
     SETTINGS query_plan_lower_array_join_function = 1,
              query_plan_enable_optimizations = 1,
              query_plan_fuse_filter_into_array_join = 1,
              use_skip_indexes = 0,
              max_rows_to_read = 128) AS full_scan,
    (SELECT arraySort(groupArray((toUInt64(id), value, hex(value), toTypeName(value))))
     FROM t_bf_fixed_array_wrappers
     ARRAY JOIN arrayConcat(arrayDistinct(fixed_values), [toFixedString(unhex('5A0051'), 4)]) AS value
     WHERE value IN (toFixedString(unhex('5A0051'), 4))
     SETTINGS query_plan_lower_array_join_function = 1,
              query_plan_enable_optimizations = 1,
              query_plan_fuse_filter_into_array_join = 1,
              use_skip_indexes = 1,
              max_rows_to_read = 128) AS indexed
SELECT 'FixedString(4) embedded-NUL unequal suffix preserves all typed rows',
       toUInt8(indexed = full_scan
       AND full_scan = arrayMap(x -> (toUInt64(x), toFixedString(unhex('5A0051'), 4), '5A005100', 'FixedString(4)'), range(1, 17)));

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_fixed_string_values') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1, 128)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'FixedString(4) embedded-NUL unequal suffix declines index and reads 16/16',
       toUInt8(name_row = 0 AND primary_key_row > 0 AND selected = 16 AND total = 16)
FROM (EXPLAIN indexes = 1
      SELECT id, value
      FROM t_bf_fixed_array_wrappers
      ARRAY JOIN arrayConcat(arrayDistinct(fixed_values), [toFixedString(unhex('5A0051'), 4)]) AS value
      WHERE value IN (toFixedString(unhex('5A0051'), 4))
      SETTINGS query_plan_lower_array_join_function = 1,
               query_plan_enable_optimizations = 1,
               query_plan_fuse_filter_into_array_join = 1,
               max_rows_to_read = 128);

DROP TABLE t_bf_signedzero;
DROP TABLE t_bf_enum_shared;
DROP TABLE t_bf_cast_numeric;
DROP TABLE t_bf_dt64_cast_text;
DROP TABLE t_bf_fixed_array_wrappers;
