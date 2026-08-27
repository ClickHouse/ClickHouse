SET enable_json_type = 1;
SET log_queries = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_stream_direct;
CREATE TABLE json_stream_direct
(
    id UInt64,
    data JSON(s String, tags Array(Nullable(String)), max_dynamic_paths = 2, max_dynamic_types = 2),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_stream_direct;
INSERT INTO json_stream_direct
SELECT number, concat('{"s":"value-', toString(number), '","tags":["common",null],"dynamic":', toString(number), '}')
FROM numbers(4)
SETTINGS log_comment = '04846_stream_direct';
INSERT INTO json_stream_direct
SELECT number, concat('{"s":"', repeat('x', 100), '-', toString(number), '","tags":["other",""]}')
FROM numbers(4, 4)
SETTINGS log_comment = '04846_stream_direct';

SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['JSONPathValuesTextIndexInputRows'])
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04846_stream_direct';

SELECT arraySort(groupArray(id)) FROM json_stream_direct WHERE data.s = 'value-2'
SETTINGS force_data_skipping_indices = 'tokens';

SYSTEM START MERGES json_stream_direct;
OPTIMIZE TABLE json_stream_direct FINAL;
CHECK TABLE json_stream_direct SETTINGS check_query_single_value_result = 1;
DROP TABLE json_stream_direct;

CREATE TABLE json_stream_invalid
(
    data JSON,
    INDEX tokens tuple(data) TYPE text(tokenizer = jsonPathValues(64))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_stream_invalid
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64), preprocessor = lower(data))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_stream_invalid
(
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64), postprocessor = lower(data))
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_stream_mixed
(
    id UInt64,
    message String,
    data JSON(s String),
    INDEX message_tokens message TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1,
    INDEX json_tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO json_stream_mixed VALUES
    (1, 'ordinary needle', '{"s":"json one"}'),
    (2, 'ordinary other', '{"s":"json needle"}');
SELECT arraySort(groupArray(id)) FROM json_stream_mixed WHERE hasToken(message, 'needle')
SETTINGS force_data_skipping_indices = 'message_tokens';
SELECT arraySort(groupArray(id)) FROM json_stream_mixed WHERE data.s = 'json needle'
SETTINGS force_data_skipping_indices = 'json_tokens';
CHECK TABLE json_stream_mixed SETTINGS check_query_single_value_result = 1;
DROP TABLE json_stream_mixed;
SET mutations_sync = 2;
SET query_plan_text_index_add_hint = 1;

SELECT 'horizontal merge';
DROP TABLE IF EXISTS json_stream_horizontal;
CREATE TABLE json_stream_horizontal
(
    id UInt64,
    data JSON(s String, max_dynamic_paths = 2),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, enable_vertical_merge_algorithm = 0;
SYSTEM STOP MERGES json_stream_horizontal;
INSERT INTO json_stream_horizontal VALUES (1, '{"s":"one"}'), (2, '{"s":"two"}'), (3, '{"s":"three"}');
INSERT INTO json_stream_horizontal VALUES (4, '{"s":"four"}'), (5, '{"s":"five"}'), (6, '{"s":"six"}');
SYSTEM START MERGES json_stream_horizontal;
OPTIMIZE TABLE json_stream_horizontal FINAL;
SYSTEM FLUSH LOGS part_log;
SELECT merge_algorithm, ProfileEvents['JSONPathValuesTextIndexInputRows']
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND database = currentDatabase() AND table = 'json_stream_horizontal' AND event_type = 'MergeParts'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT arraySort(groupArray(id)) FROM json_stream_horizontal WHERE data.s = 'five'
SETTINGS force_data_skipping_indices = 'tokens';
CHECK TABLE json_stream_horizontal SETTINGS check_query_single_value_result = 1;
DROP TABLE json_stream_horizontal;

SELECT 'vertical merge';
DROP TABLE IF EXISTS json_stream_vertical;
CREATE TABLE json_stream_vertical
(
    id UInt64,
    v1 UInt64,
    v2 String,
    v3 Array(UInt64),
    data JSON(s String, max_dynamic_paths = 2),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;
SYSTEM STOP MERGES json_stream_vertical;
INSERT INTO json_stream_vertical VALUES
    (1, 1, 'a', [1], '{"s":"one"}'),
    (2, 2, 'b', [2], '{"s":"two"}'),
    (3, 3, 'c', [3], '{"s":"three"}');
INSERT INTO json_stream_vertical VALUES
    (4, 4, 'd', [4], '{"s":"four"}'),
    (5, 5, 'e', [5], '{"s":"five"}'),
    (6, 6, 'f', [6], '{"s":"six"}');
SYSTEM START MERGES json_stream_vertical;
OPTIMIZE TABLE json_stream_vertical FINAL;
SYSTEM FLUSH LOGS part_log;
SELECT merge_algorithm, ProfileEvents['JSONPathValuesTextIndexInputRows']
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND database = currentDatabase() AND table = 'json_stream_vertical' AND event_type = 'MergeParts'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT arraySort(groupArray(id)) FROM json_stream_vertical WHERE data.s = 'two'
SETTINGS force_data_skipping_indices = 'tokens';
CHECK TABLE json_stream_vertical SETTINGS check_query_single_value_result = 1;
DROP TABLE json_stream_vertical;

SELECT 'wide materialization and mutation';
DROP TABLE IF EXISTS json_stream_materialize_wide;
CREATE TABLE json_stream_materialize_wide
(
    id UInt64,
    data JSON(s String, max_dynamic_paths = 2)
)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1;
INSERT INTO json_stream_materialize_wide VALUES
    (1, '{"s":"one"}'), (2, '{"s":"two"}'), (3, '{"s":"three"}');
ALTER TABLE json_stream_materialize_wide ADD INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1;
ALTER TABLE json_stream_materialize_wide MATERIALIZE INDEX tokens;
SYSTEM FLUSH LOGS part_log;
SELECT ProfileEvents['JSONPathValuesTextIndexInputRows']
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND database = currentDatabase() AND table = 'json_stream_materialize_wide' AND event_type = 'MutatePart'
ORDER BY event_time_microseconds DESC LIMIT 1;
ALTER TABLE json_stream_materialize_wide UPDATE data = '{"s":"updated"}' WHERE id = 1;
SELECT arraySort(groupArray(id)) FROM json_stream_materialize_wide WHERE data.s = 'updated'
SETTINGS force_data_skipping_indices = 'tokens';
CHECK TABLE json_stream_materialize_wide SETTINGS check_query_single_value_result = 1;
DROP TABLE json_stream_materialize_wide;

SELECT 'compact materialization';
DROP TABLE IF EXISTS json_stream_materialize_compact;
CREATE TABLE json_stream_materialize_compact
(
    id UInt64,
    data JSON(s String, max_dynamic_paths = 2)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 1;
INSERT INTO json_stream_materialize_compact VALUES
    (1, '{"s":"one"}'), (2, '{"s":"two"}'), (3, '{"s":"three"}');
ALTER TABLE json_stream_materialize_compact ADD INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1;
ALTER TABLE json_stream_materialize_compact MATERIALIZE INDEX tokens;
SYSTEM FLUSH LOGS part_log;
SELECT ProfileEvents['JSONPathValuesTextIndexInputRows']
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND database = currentDatabase() AND table = 'json_stream_materialize_compact' AND event_type = 'MutatePart'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT arraySort(groupArray(id)) FROM json_stream_materialize_compact WHERE data.s = 'three'
SETTINGS force_data_skipping_indices = 'tokens';
CHECK TABLE json_stream_materialize_compact SETTINGS check_query_single_value_result = 1;
DROP TABLE json_stream_materialize_compact;

SELECT 'ReplacingMergeTree survivors';
DROP TABLE IF EXISTS json_stream_replacing;
CREATE TABLE json_stream_replacing
(
    id UInt64,
    version UInt64,
    data JSON(s String),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES json_stream_replacing;
INSERT INTO json_stream_replacing VALUES
    (1, 1, '{"s":"old"}'), (2, 1, '{"s":"keep"}'), (3, 1, '{"s":"old"}');
INSERT INTO json_stream_replacing VALUES
    (1, 2, '{"s":"new"}'), (2, 0, '{"s":"stale"}'), (3, 2, '{"s":"new"}');
SYSTEM START MERGES json_stream_replacing;
OPTIMIZE TABLE json_stream_replacing FINAL;
SYSTEM FLUSH LOGS part_log;
SELECT ProfileEvents['MergedRows'], ProfileEvents['JSONPathValuesTextIndexInputRows']
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND database = currentDatabase() AND table = 'json_stream_replacing' AND event_type = 'MergeParts'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT arraySort(groupArray(id)) FROM json_stream_replacing WHERE data.s = 'new'
SETTINGS force_data_skipping_indices = 'tokens';
SELECT arraySort(groupArray(id)) FROM json_stream_replacing WHERE data.s = 'keep'
SETTINGS force_data_skipping_indices = 'tokens';
CHECK TABLE json_stream_replacing SETTINGS check_query_single_value_result = 1;
DROP TABLE json_stream_replacing;
