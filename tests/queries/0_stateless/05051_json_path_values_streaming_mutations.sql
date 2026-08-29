SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET use_skip_indexes_on_data_read = 1;
SET mutations_sync = 2;

SELECT 'wide materialization and mutation';
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
