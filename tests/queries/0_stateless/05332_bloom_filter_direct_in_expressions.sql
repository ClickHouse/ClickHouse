-- Direct-IN scalar, array, and captured-context Bloom-filter regression coverage.
-- The external test harness runs these profile-neutral assertions under serialization profiles 0 and 1.
SET max_execution_time = 15;
SET max_memory_usage = 268435456;
SET max_threads = 1;
SET max_rows_to_read = 16;
SET use_query_condition_cache = 0;
SET session_timezone = 'UTC';
SET optimize_functions_to_subcolumns = 0;
SET cast_string_to_date_time_mode = 'best_effort';

DROP TABLE IF EXISTS t_bf_direct_in_lower_name;
DROP TABLE IF EXISTS t_bf_direct_in_cast_u8_string;
DROP TABLE IF EXISTS t_bf_direct_in_year_utc;
DROP TABLE IF EXISTS t_bf_direct_in_plus_numeric;
DROP TABLE IF EXISTS t_bf_direct_in_array_distinct;
DROP TABLE IF EXISTS t_bf_direct_in_array_empty;
DROP TABLE IF EXISTS t_bf_direct_in_nested_input_collision;
DROP TABLE IF EXISTS t_bf_direct_in_cast_context;

CREATE TABLE t_bf_direct_in_lower_name
(
    id UInt8,
    name String,
    INDEX idx_direct_in_lower lower(name) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_bf_direct_in_lower_name VALUES (1, 'TARGET'),
    (2, 'noise02'), (3, 'noise03'), (4, 'noise04'), (5, 'noise05'),
    (6, 'noise06'), (7, 'noise07'), (8, 'noise08'), (9, 'noise09'),
    (10, 'noise10'), (11, 'noise11'), (12, 'noise12'), (13, 'noise13'),
    (14, 'noise14'), (15, 'noise15'), (16, 'target');

SELECT 'DIRECT_IN_LOWER_READBACK_BEGIN' FORMAT TSVRaw;
SELECT id, name, lower(name), toTypeName(name), toTypeName(lower(name)) FROM t_bf_direct_in_lower_name ORDER BY id FORMAT TSVRaw;
SELECT 'DIRECT_IN_LOWER_READBACK_END' FORMAT TSVRaw;

CREATE TABLE t_bf_direct_in_cast_u8_string
(
    id UInt8,
    x UInt8,
    INDEX idx_direct_in_cast_u8_string CAST(x, 'String') TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_bf_direct_in_cast_u8_string VALUES (1, 7), (2, 102), (3, 103), (4, 104), (5, 105), (6, 106), (7, 107), (8, 108), (9, 109), (10, 110), (11, 111), (12, 112), (13, 113), (14, 114), (15, 115), (16, 7);

SELECT 'DIRECT_IN_CAST_U8_READBACK_BEGIN' FORMAT TSVRaw;
SELECT id, x, CAST(x, 'String'), toTypeName(x), toTypeName(CAST(x, 'String')) FROM t_bf_direct_in_cast_u8_string ORDER BY id FORMAT TSVRaw;
SELECT 'DIRECT_IN_CAST_U8_READBACK_END' FORMAT TSVRaw;

CREATE TABLE t_bf_direct_in_year_utc
(
    id UInt8,
    ts DateTime('UTC'),
    INDEX idx_direct_in_year toYear(ts) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_bf_direct_in_year_utc VALUES
    (1, toDateTime('2024-01-01 00:00:00', 'UTC')),
    (2, toDateTime('2010-01-01 00:00:00', 'UTC')),
    (3, toDateTime('2011-01-01 00:00:00', 'UTC')),
    (4, toDateTime('2012-01-01 00:00:00', 'UTC')),
    (5, toDateTime('2013-01-01 00:00:00', 'UTC')),
    (6, toDateTime('2014-01-01 00:00:00', 'UTC')),
    (7, toDateTime('2015-01-01 00:00:00', 'UTC')),
    (8, toDateTime('2016-01-01 00:00:00', 'UTC')),
    (9, toDateTime('2017-01-01 00:00:00', 'UTC')),
    (10, toDateTime('2018-01-01 00:00:00', 'UTC')),
    (11, toDateTime('2019-01-01 00:00:00', 'UTC')),
    (12, toDateTime('2020-01-01 00:00:00', 'UTC')),
    (13, toDateTime('2021-01-01 00:00:00', 'UTC')),
    (14, toDateTime('2022-01-01 00:00:00', 'UTC')),
    (15, toDateTime('2023-01-01 00:00:00', 'UTC')),
    (16, toDateTime('2024-12-31 23:59:59', 'UTC'));

SELECT 'DIRECT_IN_YEAR_READBACK_BEGIN' FORMAT TSVRaw;
SELECT id, toYear(ts), toTypeName(ts), toTypeName(toYear(ts)) FROM t_bf_direct_in_year_utc ORDER BY id FORMAT TSVRaw;
SELECT 'DIRECT_IN_YEAR_READBACK_END' FORMAT TSVRaw;

CREATE TABLE t_bf_direct_in_plus_numeric
(
    id UInt8,
    x UInt64,
    INDEX idx_direct_in_plus plus(x, 1) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_bf_direct_in_plus_numeric VALUES (1, 1), (2, 102), (3, 103), (4, 104), (5, 105), (6, 106), (7, 107), (8, 108), (9, 109), (10, 110), (11, 111), (12, 112), (13, 113), (14, 114), (15, 115), (16, 1);

SELECT 'DIRECT_IN_PLUS_READBACK_BEGIN' FORMAT TSVRaw;
SELECT id, x, plus(x, 1), toTypeName(x), toTypeName(plus(x, 1)) FROM t_bf_direct_in_plus_numeric ORDER BY id FORMAT TSVRaw;
SELECT 'DIRECT_IN_PLUS_READBACK_END' FORMAT TSVRaw;

CREATE TABLE t_bf_direct_in_array_distinct
(
    id UInt8,
    tags Array(String),
    INDEX idx_direct_in_array_distinct arrayDistinct(tags) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_bf_direct_in_array_distinct VALUES
    (1, ['target', 'target']),
    (2, ['noise02', 'noise02']),
    (3, ['noise03', 'noise03']),
    (4, ['noise04', 'noise04']),
    (5, ['noise05', 'noise05']),
    (6, ['noise06', 'noise06']),
    (7, ['noise07', 'noise07']),
    (8, ['noise08', 'noise08']),
    (9, ['noise09', 'noise09']),
    (10, ['noise10', 'noise10']),
    (11, ['noise11', 'noise11']),
    (12, ['noise12', 'noise12']),
    (13, ['noise13', 'noise13']),
    (14, ['noise14', 'noise14']),
    (15, ['noise15', 'noise15']),
    (16, ['target', 'target']);

SELECT 'DIRECT_IN_ARRAY_READBACK_BEGIN' FORMAT TSVRaw;
SELECT id, tags, arrayDistinct(tags), toTypeName(tags), toTypeName(arrayDistinct(tags)) FROM t_bf_direct_in_array_distinct ORDER BY id FORMAT TSVRaw;
SELECT 'DIRECT_IN_ARRAY_READBACK_END' FORMAT TSVRaw;

CREATE TABLE t_bf_direct_in_array_empty
(
    id UInt8,
    tags Array(String),
    INDEX idx_direct_in_array_empty arrayDistinct(tags) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_bf_direct_in_array_empty VALUES (1, []), (2, ['noise02', 'noise02']), (3, []), (4, ['noise04', 'noise04']);

SELECT 'DIRECT_IN_ARRAY_EMPTY_READBACK_BEGIN' FORMAT TSVRaw;
SELECT id, tags, arrayDistinct(tags), toTypeName(tags), toTypeName(arrayDistinct(tags)) FROM t_bf_direct_in_array_empty ORDER BY id FORMAT TSVRaw;
SELECT 'DIRECT_IN_ARRAY_EMPTY_READBACK_END' FORMAT TSVRaw;

CREATE TABLE t_bf_direct_in_nested_input_collision
(
    id UInt8,
    tags Array(String),
    `arrayDistinct(tags)` Array(String),
    INDEX idx_direct_in_length length(arrayDistinct(tags)) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_bf_direct_in_nested_input_collision VALUES
    (1, ['a', 'a'], ['x', 'y']),
    (2, ['b', 'c'], ['z']),
    (3, ['d', 'd', 'd'], ['u', 'v']),
    (4, ['e', 'f', 'g'], ['w']);

SELECT 'DIRECT_IN_NESTED_READBACK_BEGIN' FORMAT TSVRaw;
SELECT id, tags, `arrayDistinct(tags)`, arrayDistinct(tags), length(arrayDistinct(tags)), length(`arrayDistinct(tags)`), toTypeName(tags), toTypeName(`arrayDistinct(tags)`), toTypeName(arrayDistinct(tags)) FROM t_bf_direct_in_nested_input_collision ORDER BY id FORMAT TSVRaw;
SELECT 'DIRECT_IN_NESTED_READBACK_END' FORMAT TSVRaw;

SET cast_string_to_date_time_mode = 'best_effort';
CREATE TABLE t_bf_direct_in_cast_context
(
    id UInt8,
    s String,
    INDEX idx_direct_in_cast_context CAST(s AS DateTime('UTC')) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_bf_direct_in_cast_context VALUES
    (1, '2020-02-01 20:00:00'),
    (2, '2020-05-02 20:00:00'),
    (3, '2020-05-03 20:00:00'),
    (4, '2020-05-04 20:00:00'),
    (5, '2020-05-05 20:00:00'),
    (6, '2020-05-06 20:00:00'),
    (7, '2020-05-07 20:00:00'),
    (8, '2020-05-08 20:00:00'),
    (9, '2020-05-09 20:00:00'),
    (10, '2020-05-10 20:00:00'),
    (11, '2020-05-11 20:00:00'),
    (12, '2020-05-12 20:00:00'),
    (13, '2020-05-13 20:00:00'),
    (14, '2020-05-14 20:00:00'),
    (15, '2020-05-15 20:00:00'),
    (16, '2020-02-01 20:00:00');

SELECT 'DIRECT_IN_CAST_BEST_EFFORT_CONTEXT_BEGIN' FORMAT TSVRaw;
SELECT getSetting('cast_string_to_date_time_mode'), timezone() FORMAT TSVRaw;
SELECT 'DIRECT_IN_CAST_BEST_EFFORT_CONTEXT_END' FORMAT TSVRaw;
SELECT 'DIRECT_IN_CAST_BEST_EFFORT_READBACK_BEGIN' FORMAT TSVRaw;
SELECT id, s, CAST(s AS DateTime('UTC')), toTypeName(CAST(s AS DateTime('UTC'))) FROM t_bf_direct_in_cast_context ORDER BY id FORMAT TSVRaw;
SELECT 'DIRECT_IN_CAST_BEST_EFFORT_READBACK_END' FORMAT TSVRaw;

SET cast_string_to_date_time_mode = 'best_effort_us';
SELECT 'DIRECT_IN_CAST_BEST_EFFORT_US_CONTEXT_BEGIN' FORMAT TSVRaw;
SELECT getSetting('cast_string_to_date_time_mode'), timezone() FORMAT TSVRaw;
SELECT 'DIRECT_IN_CAST_BEST_EFFORT_US_CONTEXT_END' FORMAT TSVRaw;
SELECT 'DIRECT_IN_CAST_BEST_EFFORT_US_READBACK_BEGIN' FORMAT TSVRaw;
SELECT id, s, CAST(s AS DateTime('UTC')), toTypeName(CAST(s AS DateTime('UTC'))) FROM t_bf_direct_in_cast_context ORDER BY id FORMAT TSVRaw;
SELECT 'DIRECT_IN_CAST_BEST_EFFORT_US_READBACK_END' FORMAT TSVRaw;

SET cast_string_to_date_time_mode = 'best_effort';
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_lower') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'lower: named index prunes 2/16', has_index AND selected = 2 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_lower_name WHERE lower(name) IN ('target') SETTINGS use_skip_indexes = 1, max_rows_to_read = 16) SETTINGS max_rows_to_read = 128;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_cast_u8_string') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'UInt8 CAST-to-String: named index prunes 2/16', has_index AND selected = 2 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_cast_u8_string WHERE CAST(x, 'String') IN ('7') SETTINGS use_skip_indexes = 1, max_rows_to_read = 16) SETTINGS max_rows_to_read = 128;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_year') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'toYear UTC: named index prunes 2/16', has_index AND selected = 2 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_year_utc WHERE toYear(ts) IN (2024) SETTINGS use_skip_indexes = 1, max_rows_to_read = 16) SETTINGS max_rows_to_read = 128;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_plus') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'plus(x,1): named index prunes 2/16', has_index AND selected = 2 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_plus_numeric WHERE plus(x, 1) IN (2) SETTINGS use_skip_indexes = 1, max_rows_to_read = 16) SETTINGS max_rows_to_read = 128;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_array_distinct') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'arrayDistinct direct Array result: named index prunes 2/16', has_index AND selected = 2 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_array_distinct WHERE arrayDistinct(tags) IN (['target']) SETTINGS use_skip_indexes = 1, max_rows_to_read = 16) SETTINGS max_rows_to_read = 128;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_length') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'nested length(arrayDistinct) FUNCTION control: named index prunes 1/4', has_index AND selected = 1 AND total = 4
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_nested_input_collision WHERE length(arrayDistinct(tags)) IN (3) SETTINGS use_skip_indexes = 1, max_rows_to_read = 4) SETTINGS max_rows_to_read = 128;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_cast_context') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'CAST persisted context unknown: declines named index, reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_cast_context WHERE CAST(s AS DateTime('UTC')) IN (toDateTime('2020-02-01 20:00:00', 'UTC')) SETTINGS use_skip_indexes = 1, max_rows_to_read = 16) SETTINGS max_rows_to_read = 128;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_array_empty') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS primary_granules_row,
    toUInt64OrZero(extract(primary_granules_row, 'Granules: ([0-9]+)/')) AS primary_selected,
    toUInt64OrZero(extract(primary_granules_row, 'Granules: [0-9]+/([0-9]+)')) AS primary_total
SELECT 'empty Array IN declines index, reads 4/4', name_row = 0 AND primary_selected = 4 AND primary_total = 4
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_array_empty WHERE arrayDistinct(tags) IN ([]) SETTINGS use_skip_indexes = 1, max_rows_to_read = 4) SETTINGS max_rows_to_read = 128;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_length') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS primary_granules_row,
    toUInt64OrZero(extract(primary_granules_row, 'Granules: ([0-9]+)/')) AS primary_selected,
    toUInt64OrZero(extract(primary_granules_row, 'Granules: [0-9]+/([0-9]+)')) AS primary_total
SELECT 'quoted INPUT collision declines idx_direct_in_length, reads 4/4', name_row = 0 AND primary_selected = 4 AND primary_total = 4
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_nested_input_collision WHERE length(`arrayDistinct(tags)`) IN (2) SETTINGS use_skip_indexes = 1, max_rows_to_read = 4) SETTINGS max_rows_to_read = 128;

SET cast_string_to_date_time_mode = 'best_effort_us';
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_cast_context') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS primary_granules_row,
    toUInt64OrZero(extract(primary_granules_row, 'Granules: ([0-9]+)/')) AS primary_selected,
    toUInt64OrZero(extract(primary_granules_row, 'Granules: [0-9]+/([0-9]+)')) AS primary_total
SELECT 'CAST best_effort_us mismatch declines idx_direct_in_cast_context, reads 16/16', name_row = 0 AND primary_selected = 16 AND primary_total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_cast_context WHERE CAST(s AS DateTime('UTC')) IN (toDateTime('2020-02-01 20:00:00', 'UTC')) SETTINGS use_skip_indexes = 1, max_rows_to_read = 16) SETTINGS max_rows_to_read = 128;

SET max_rows_to_read = 32;
WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), lower(name)))) FROM t_bf_direct_in_lower_name WHERE lower(name) IN ('target') SETTINGS use_skip_indexes = 0, max_rows_to_read = 32) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), lower(name)))) FROM t_bf_direct_in_lower_name WHERE lower(name) IN ('target') SETTINGS use_skip_indexes = 1, max_rows_to_read = 32) AS indexed
SELECT 'lower full OFF/ON multiset', full_scan = indexed AND full_scan = [(toUInt64(1), 'target'), (toUInt64(16), 'target')] FORMAT TSVRaw;

WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(x, 'String')))) FROM t_bf_direct_in_cast_u8_string WHERE CAST(x, 'String') IN ('7') SETTINGS use_skip_indexes = 0, max_rows_to_read = 32) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(x, 'String')))) FROM t_bf_direct_in_cast_u8_string WHERE CAST(x, 'String') IN ('7') SETTINGS use_skip_indexes = 1, max_rows_to_read = 32) AS indexed
SELECT 'UInt8 CAST-to-String full OFF/ON multiset', full_scan = indexed AND full_scan = [(toUInt64(1), '7'), (toUInt64(16), '7')] FORMAT TSVRaw;

WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), toUInt64(toYear(ts))))) FROM t_bf_direct_in_year_utc WHERE toYear(ts) IN (2024) SETTINGS use_skip_indexes = 0, max_rows_to_read = 32) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), toUInt64(toYear(ts))))) FROM t_bf_direct_in_year_utc WHERE toYear(ts) IN (2024) SETTINGS use_skip_indexes = 1, max_rows_to_read = 32) AS indexed
SELECT 'toYear full OFF/ON multiset', full_scan = indexed AND full_scan = [(toUInt64(1), toUInt64(2024)), (toUInt64(16), toUInt64(2024))] FORMAT TSVRaw;

WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), toUInt64(plus(x, 1))))) FROM t_bf_direct_in_plus_numeric WHERE plus(x, 1) IN (2) SETTINGS use_skip_indexes = 0, max_rows_to_read = 32) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), toUInt64(plus(x, 1))))) FROM t_bf_direct_in_plus_numeric WHERE plus(x, 1) IN (2) SETTINGS use_skip_indexes = 1, max_rows_to_read = 32) AS indexed
SELECT 'plus(x,1) full OFF/ON multiset', full_scan = indexed AND full_scan = [(toUInt64(1), toUInt64(2)), (toUInt64(16), toUInt64(2))] FORMAT TSVRaw;

WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), arrayDistinct(tags)))) FROM t_bf_direct_in_array_distinct WHERE arrayDistinct(tags) IN (['target']) SETTINGS use_skip_indexes = 0, max_rows_to_read = 32) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), arrayDistinct(tags)))) FROM t_bf_direct_in_array_distinct WHERE arrayDistinct(tags) IN (['target']) SETTINGS use_skip_indexes = 1, max_rows_to_read = 32) AS indexed
SELECT 'arrayDistinct full OFF/ON multiset', full_scan = indexed AND full_scan = [(toUInt64(1), ['target']), (toUInt64(16), ['target'])] FORMAT TSVRaw;

SET max_rows_to_read = 8;
WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), arrayDistinct(tags)))) FROM t_bf_direct_in_array_empty WHERE arrayDistinct(tags) IN ([]) SETTINGS use_skip_indexes = 0, max_rows_to_read = 8) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), arrayDistinct(tags)))) FROM t_bf_direct_in_array_empty WHERE arrayDistinct(tags) IN ([]) SETTINGS use_skip_indexes = 1, max_rows_to_read = 8) AS indexed
SELECT 'empty Array IN full OFF/ON multiset', full_scan = indexed AND full_scan = [(toUInt64(1), emptyArrayString()), (toUInt64(3), emptyArrayString())] FORMAT TSVRaw;

WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), toUInt64(length(arrayDistinct(tags)))))) FROM t_bf_direct_in_nested_input_collision WHERE length(arrayDistinct(tags)) IN (3) SETTINGS use_skip_indexes = 0, max_rows_to_read = 8) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), toUInt64(length(arrayDistinct(tags)))))) FROM t_bf_direct_in_nested_input_collision WHERE length(arrayDistinct(tags)) IN (3) SETTINGS use_skip_indexes = 1, max_rows_to_read = 8) AS indexed
SELECT 'nested FUNCTION positive full OFF/ON multiset', full_scan = indexed AND full_scan = [(toUInt64(4), toUInt64(3))] FORMAT TSVRaw;

WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), toUInt64(length(`arrayDistinct(tags)`))))) FROM t_bf_direct_in_nested_input_collision WHERE length(`arrayDistinct(tags)`) IN (2) SETTINGS use_skip_indexes = 0, max_rows_to_read = 8) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), toUInt64(length(`arrayDistinct(tags)`))))) FROM t_bf_direct_in_nested_input_collision WHERE length(`arrayDistinct(tags)`) IN (2) SETTINGS use_skip_indexes = 1, max_rows_to_read = 8) AS indexed
SELECT 'quoted INPUT collision full OFF/ON multiset', full_scan = indexed AND full_scan = [(toUInt64(1), toUInt64(2)), (toUInt64(3), toUInt64(2))] FORMAT TSVRaw;

SET max_rows_to_read = 32;
SET cast_string_to_date_time_mode = 'best_effort';
WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(s AS DateTime('UTC'))))) FROM t_bf_direct_in_cast_context WHERE CAST(s AS DateTime('UTC')) IN (toDateTime('2020-02-01 20:00:00', 'UTC')) SETTINGS use_skip_indexes = 0, max_rows_to_read = 32) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(s AS DateTime('UTC'))))) FROM t_bf_direct_in_cast_context WHERE CAST(s AS DateTime('UTC')) IN (toDateTime('2020-02-01 20:00:00', 'UTC')) SETTINGS use_skip_indexes = 1, max_rows_to_read = 32) AS indexed
SELECT 'CAST same best_effort context full OFF/ON multiset', full_scan = indexed AND full_scan = [(toUInt64(1), toDateTime('2020-02-01 20:00:00', 'UTC')), (toUInt64(16), toDateTime('2020-02-01 20:00:00', 'UTC'))] FORMAT TSVRaw;

SET cast_string_to_date_time_mode = 'best_effort_us';
WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(s AS DateTime('UTC'))))) FROM t_bf_direct_in_cast_context WHERE CAST(s AS DateTime('UTC')) IN (toDateTime('2020-02-01 20:00:00', 'UTC')) SETTINGS use_skip_indexes = 0, max_rows_to_read = 32) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(s AS DateTime('UTC'))))) FROM t_bf_direct_in_cast_context WHERE CAST(s AS DateTime('UTC')) IN (toDateTime('2020-02-01 20:00:00', 'UTC')) SETTINGS use_skip_indexes = 1, max_rows_to_read = 32) AS indexed
SELECT 'CAST best_effort_us mismatch full OFF/ON multiset', full_scan = indexed AND full_scan = [(toUInt64(1), toDateTime('2020-02-01 20:00:00', 'UTC')), (toUInt64(16), toDateTime('2020-02-01 20:00:00', 'UTC'))] FORMAT TSVRaw;

SET max_rows_to_read = 16;
SET cast_string_to_date_time_mode = 'best_effort';

-- Exercises the direct scalar-index matcher with Nullable Set input. With
-- transform_null_in=0, Set construction strips NULL and provides a same-shape
-- pruning control. With transform_null_in=1, IN/GLOBAL IN become nullIn forms
-- and preserve the NULL-bearing Set; the non-Nullable Bloom index must decline.
DROP TABLE IF EXISTS t_bf_direct_in_nullable_rhs;
CREATE TABLE t_bf_direct_in_nullable_rhs (value Nullable(String)) ENGINE = Memory;
INSERT INTO t_bf_direct_in_nullable_rhs VALUES ('target'), (NULL);

SELECT 'DIRECT_IN_NULLABLE_RHS_READBACK_BEGIN' FORMAT TSVRaw;
SELECT value, toTypeName(value), isNull(value)
FROM t_bf_direct_in_nullable_rhs ORDER BY isNull(value) FORMAT TSVRaw;
SELECT 'DIRECT_IN_NULLABLE_RHS_READBACK_END' FORMAT TSVRaw;
SELECT toTypeName(toNullable(lower(name)))
FROM t_bf_direct_in_lower_name LIMIT 1 FORMAT TSVRaw;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_lower') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Nullable-LHS IN null-stripped Set: named index prunes 2/16', has_index AND selected = 2 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_lower_name
      WHERE toNullable(lower(name)) IN (SELECT value FROM t_bf_direct_in_nullable_rhs)
      SETTINGS transform_null_in = 0, use_skip_indexes = 1, max_rows_to_read = 16)
SETTINGS max_rows_to_read = 128;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_lower') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Nullable-LHS GLOBAL IN null-stripped Set: named index prunes 2/16', has_index AND selected = 2 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_lower_name
      WHERE toNullable(lower(name)) GLOBAL IN (SELECT value FROM t_bf_direct_in_nullable_rhs)
      SETTINGS transform_null_in = 0, use_skip_indexes = 1, max_rows_to_read = 16)
SETTINGS max_rows_to_read = 128;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_lower') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Nullable-LHS IN NULL-bearing Set: declines named index, reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_lower_name
      WHERE toNullable(lower(name)) IN (SELECT value FROM t_bf_direct_in_nullable_rhs)
      SETTINGS transform_null_in = 1, use_skip_indexes = 1, max_rows_to_read = 16)
SETTINGS max_rows_to_read = 128;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_lower') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Nullable-LHS GLOBAL IN NULL-bearing Set: declines named index, reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_lower_name
      WHERE toNullable(lower(name)) GLOBAL IN (SELECT value FROM t_bf_direct_in_nullable_rhs)
      SETTINGS transform_null_in = 1, use_skip_indexes = 1, max_rows_to_read = 16)
SETTINGS max_rows_to_read = 128;

SET max_rows_to_read = 40;
WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), lower(name)))) FROM t_bf_direct_in_lower_name
     WHERE toNullable(lower(name)) IN (SELECT value FROM t_bf_direct_in_nullable_rhs)
     SETTINGS transform_null_in = 1, use_skip_indexes = 0, max_rows_to_read = 40) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), lower(name)))) FROM t_bf_direct_in_lower_name
     WHERE toNullable(lower(name)) IN (SELECT value FROM t_bf_direct_in_nullable_rhs)
     SETTINGS transform_null_in = 1, use_skip_indexes = 1, max_rows_to_read = 40) AS indexed
SELECT 'Nullable-LHS IN NULL-bearing Set full OFF/ON exact multiset',
       full_scan = indexed AND full_scan = [(toUInt64(1), 'target'), (toUInt64(16), 'target')]
FORMAT TSVRaw;

WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), lower(name)))) FROM t_bf_direct_in_lower_name
     WHERE toNullable(lower(name)) GLOBAL IN (SELECT value FROM t_bf_direct_in_nullable_rhs)
     SETTINGS transform_null_in = 1, use_skip_indexes = 0, max_rows_to_read = 40) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), lower(name)))) FROM t_bf_direct_in_lower_name
     WHERE toNullable(lower(name)) GLOBAL IN (SELECT value FROM t_bf_direct_in_nullable_rhs)
     SETTINGS transform_null_in = 1, use_skip_indexes = 1, max_rows_to_read = 40) AS indexed
SELECT 'Nullable-LHS GLOBAL IN NULL-bearing Set full OFF/ON exact multiset',
       full_scan = indexed AND full_scan = [(toUInt64(1), 'target'), (toUInt64(16), 'target')]
FORMAT TSVRaw;

-- Format-only context members must not hide the equivalent scalar CAST index.
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_cast_u8_string') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'UInt8 CAST allow-special serialization flag 0 keeps named index at 2/16', name_row > 0 AND selected = 2 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_cast_u8_string
      WHERE CAST(x, 'String') IN ('7')
      SETTINGS allow_special_serialization_kinds_in_output_formats = 0, use_skip_indexes = 1, max_rows_to_read = 16)
SETTINGS max_rows_to_read = 128;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_cast_u8_string') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'UInt8 CAST parallel-formatting flag 0 keeps named index at 2/16', name_row > 0 AND selected = 2 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_cast_u8_string
      WHERE CAST(x, 'String') IN ('7')
      SETTINGS output_format_parallel_formatting = 0, use_skip_indexes = 1, max_rows_to_read = 16)
SETTINGS max_rows_to_read = 128;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_cast_context') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'String-to-DateTime format flags cannot prove persisted context, reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_cast_context
      WHERE CAST(s AS DateTime('UTC')) IN (toDateTime('2020-02-01 20:00:00', 'UTC'))
      SETTINGS allow_special_serialization_kinds_in_output_formats = 0,
               input_format_parquet_dictionary_filter_push_down = 0,
               output_format_parallel_formatting = 0,
               input_format_parallel_parsing = 0,
               cast_string_to_date_time_mode = 'best_effort',
               use_skip_indexes = 1, max_rows_to_read = 16)
SETTINGS max_rows_to_read = 128;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_direct_in_cast_u8_string') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'UInt8 CAST precise-float context mismatch declines named index, reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_direct_in_cast_u8_string
      WHERE CAST(x, 'String') IN ('7')
      SETTINGS precise_float_parsing = 0, use_skip_indexes = 1, max_rows_to_read = 16)
SETTINGS max_rows_to_read = 128;

WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(x, 'String'))))
     FROM t_bf_direct_in_cast_u8_string WHERE CAST(x, 'String') IN ('7')
     SETTINGS allow_special_serialization_kinds_in_output_formats = 0, use_skip_indexes = 0, max_rows_to_read = 32) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(x, 'String'))))
     FROM t_bf_direct_in_cast_u8_string WHERE CAST(x, 'String') IN ('7')
     SETTINGS allow_special_serialization_kinds_in_output_formats = 0, use_skip_indexes = 1, max_rows_to_read = 32) AS indexed
SELECT 'UInt8 CAST allow-special serialization flag 0 full OFF/ON typed rows',
       full_scan = indexed AND full_scan = [(toUInt64(1), '7'), (toUInt64(16), '7')]
FORMAT TSVRaw;

WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(x, 'String'))))
     FROM t_bf_direct_in_cast_u8_string WHERE CAST(x, 'String') IN ('7')
     SETTINGS output_format_parallel_formatting = 0, use_skip_indexes = 0, max_rows_to_read = 32) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(x, 'String'))))
     FROM t_bf_direct_in_cast_u8_string WHERE CAST(x, 'String') IN ('7')
     SETTINGS output_format_parallel_formatting = 0, use_skip_indexes = 1, max_rows_to_read = 32) AS indexed
SELECT 'UInt8 CAST parallel-formatting flag 0 full OFF/ON typed rows',
       full_scan = indexed AND full_scan = [(toUInt64(1), '7'), (toUInt64(16), '7')]
FORMAT TSVRaw;

WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(x, 'String'))))
     FROM t_bf_direct_in_cast_u8_string WHERE CAST(x, 'String') IN ('7')
     SETTINGS allow_special_serialization_kinds_in_output_formats = 0,
              output_format_parallel_formatting = 0, use_skip_indexes = 0, max_rows_to_read = 32) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(x, 'String'))))
     FROM t_bf_direct_in_cast_u8_string WHERE CAST(x, 'String') IN ('7')
     SETTINGS allow_special_serialization_kinds_in_output_formats = 0,
              output_format_parallel_formatting = 0, use_skip_indexes = 1, max_rows_to_read = 32) AS indexed
SELECT 'UInt8 CAST two format flags 0 full OFF/ON typed rows',
       full_scan = indexed AND full_scan = [(toUInt64(1), '7'), (toUInt64(16), '7')]
FORMAT TSVRaw;

SET cast_string_to_date_time_mode = 'best_effort';
WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(s AS DateTime('UTC')))))
     FROM t_bf_direct_in_cast_context
     WHERE CAST(s AS DateTime('UTC')) IN (toDateTime('2020-02-01 20:00:00', 'UTC'))
     SETTINGS allow_special_serialization_kinds_in_output_formats = 0,
              input_format_parquet_dictionary_filter_push_down = 0,
              output_format_parallel_formatting = 0,
              input_format_parallel_parsing = 0,
              cast_string_to_date_time_mode = 'best_effort',
              use_skip_indexes = 0, max_rows_to_read = 32) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(s AS DateTime('UTC')))))
     FROM t_bf_direct_in_cast_context
     WHERE CAST(s AS DateTime('UTC')) IN (toDateTime('2020-02-01 20:00:00', 'UTC'))
     SETTINGS allow_special_serialization_kinds_in_output_formats = 0,
              input_format_parquet_dictionary_filter_push_down = 0,
              output_format_parallel_formatting = 0,
              input_format_parallel_parsing = 0,
              cast_string_to_date_time_mode = 'best_effort',
              use_skip_indexes = 1, max_rows_to_read = 32) AS indexed
SELECT 'String-to-DateTime four format flags 0 full OFF/ON typed rows',
       full_scan = indexed AND full_scan = [(toUInt64(1), toDateTime('2020-02-01 20:00:00', 'UTC')),
                                            (toUInt64(16), toDateTime('2020-02-01 20:00:00', 'UTC'))]
FORMAT TSVRaw;

WITH
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(x, 'String'))))
     FROM t_bf_direct_in_cast_u8_string WHERE CAST(x, 'String') IN ('7')
     SETTINGS precise_float_parsing = 0, use_skip_indexes = 0, max_rows_to_read = 32) AS full_scan,
    (SELECT arraySort(groupArray(tuple(toUInt64(id), CAST(x, 'String'))))
     FROM t_bf_direct_in_cast_u8_string WHERE CAST(x, 'String') IN ('7')
     SETTINGS precise_float_parsing = 0, use_skip_indexes = 1, max_rows_to_read = 32) AS indexed
SELECT 'UInt8 CAST precise-float context mismatch full OFF/ON typed rows',
       full_scan = indexed AND full_scan = [(toUInt64(1), '7'), (toUInt64(16), '7')]
FORMAT TSVRaw;

SET max_rows_to_read = 16;
DROP TABLE IF EXISTS t_bf_direct_in_nullable_rhs;

DROP TABLE IF EXISTS t_bf_direct_in_lower_name;
DROP TABLE IF EXISTS t_bf_direct_in_cast_u8_string;
DROP TABLE IF EXISTS t_bf_direct_in_year_utc;
DROP TABLE IF EXISTS t_bf_direct_in_plus_numeric;
DROP TABLE IF EXISTS t_bf_direct_in_array_distinct;
DROP TABLE IF EXISTS t_bf_direct_in_array_empty;
DROP TABLE IF EXISTS t_bf_direct_in_nested_input_collision;
DROP TABLE IF EXISTS t_bf_direct_in_cast_context;

-- Primitive integer conversion is independent of input parsing settings, including wrapping values.
SET input_format_parallel_parsing = 1;
SET max_rows_to_read = 128;
DROP TABLE IF EXISTS t_bf_direct_in_to_uint8;
CREATE TABLE t_bf_direct_in_to_uint8
(
    id UInt64,
    x Int64,
    u UInt64,
    INDEX idx_signed_to_uint8 toUInt8(x) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_unsigned_to_uint8 toUInt8(u) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_bf_direct_in_to_uint8
SELECT number,
       multiIf(number = 3, toInt64(-253), number = 9, toInt64(259), toInt64(1000 + number)),
       multiIf(number = 3, toUInt64(259), number = 9, toUInt64(9), toUInt64(1000 + number))
FROM numbers(16)
SETTINGS input_format_parallel_parsing = 1;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_signed_to_uint8') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'signed toUInt8 IN across parsing settings prunes 2/16', name_row > 0 AND selected = 2 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_direct_in_to_uint8 WHERE toUInt8(x) IN (3)
      SETTINGS input_format_parallel_parsing = 0, use_skip_indexes = 1);

WITH
    (SELECT arraySort(groupArray(tuple(id, x, toUInt8(x)))) FROM t_bf_direct_in_to_uint8 WHERE toUInt8(x) IN (3)
     SETTINGS input_format_parallel_parsing = 0, use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(tuple(id, x, toUInt8(x)))) FROM t_bf_direct_in_to_uint8 WHERE toUInt8(x) IN (3)
     SETTINGS input_format_parallel_parsing = 0, use_skip_indexes = 1) AS indexed
SELECT 'signed toUInt8 negative and wrapped inputs preserve typed rows',
       full_scan = indexed AND full_scan = [(toUInt64(3), toInt64(-253), toUInt8(3)), (toUInt64(9), toInt64(259), toUInt8(3))];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_unsigned_to_uint8') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'unsigned toUInt8 IN across parsing settings prunes 2/16', name_row > 0 AND selected = 2 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_direct_in_to_uint8 WHERE toUInt8(u) IN (3, 9)
      SETTINGS input_format_parallel_parsing = 0, use_skip_indexes = 1);

WITH
    (SELECT arraySort(groupArray(tuple(id, u, toUInt8(u)))) FROM t_bf_direct_in_to_uint8 WHERE toUInt8(u) IN (3, 9)
     SETTINGS input_format_parallel_parsing = 0, use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(tuple(id, u, toUInt8(u)))) FROM t_bf_direct_in_to_uint8 WHERE toUInt8(u) IN (3, 9)
     SETTINGS input_format_parallel_parsing = 0, use_skip_indexes = 1) AS indexed
SELECT 'unsigned toUInt8 in-range and wrapped inputs preserve typed rows',
       full_scan = indexed AND full_scan = [(toUInt64(3), toUInt64(259), toUInt8(3)), (toUInt64(9), toUInt64(9), toUInt8(9))];
DROP TABLE t_bf_direct_in_to_uint8;
