SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

SELECT 'horizontal merge';
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
