SET enable_json_type = 1;
SET mutations_sync = 2, alter_sync = 2;

DROP TABLE IF EXISTS json_path_values_per_part_config;

CREATE TABLE json_path_values_per_part_config
(
    id UInt64,
    data JSON(max_dynamic_paths = 0, old_path String, new_path String),
    INDEX idx data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_per_part_config VALUES (1, '{"old_path":"old","new_path":"old-hit"}');

SYSTEM STOP MERGES json_path_values_per_part_config;
ALTER TABLE json_path_values_per_part_config DETACH PART 'all_1_1_0';
ALTER TABLE json_path_values_per_part_config DROP INDEX idx;
ALTER TABLE json_path_values_per_part_config ADD INDEX idx data TYPE text(tokenizer = jsonPathValues(128)) GRANULARITY 1;
ALTER TABLE json_path_values_per_part_config ATTACH PART 'all_1_1_0';

INSERT INTO json_path_values_per_part_config VALUES (2, '{"old_path":"new","new_path":"new-hit"}');

SELECT groupArray(id) FROM json_path_values_per_part_config WHERE data.new_path = 'old-hit'
SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 1;
SELECT groupArray(id) FROM json_path_values_per_part_config WHERE data.new_path = 'new-hit'
SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 1;
SELECT count() FROM json_path_values_per_part_config WHERE data.new_path = 'missing'
SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 1;

SYSTEM START MERGES json_path_values_per_part_config;
OPTIMIZE TABLE json_path_values_per_part_config FINAL;

SELECT groupArray(id) FROM json_path_values_per_part_config WHERE data.new_path = 'old-hit'
SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 1;
SELECT groupArray(id) FROM json_path_values_per_part_config WHERE data.new_path = 'new-hit'
SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 1;
SELECT count() FROM json_path_values_per_part_config WHERE data.new_path = 'missing'
SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 1;

DROP TABLE json_path_values_per_part_config;

CREATE TABLE json_path_values_per_part_config
(
    id UInt64,
    data JSON(max_dynamic_paths = 0, value UInt8),
    INDEX idx data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_per_part_config VALUES (1, '{"value":1}');

SYSTEM STOP MERGES json_path_values_per_part_config;
ALTER TABLE json_path_values_per_part_config DETACH PART 'all_1_1_0';
ALTER TABLE json_path_values_per_part_config MODIFY COLUMN data JSON(max_dynamic_paths = 0, value Bool);
ALTER TABLE json_path_values_per_part_config ATTACH PART 'all_1_1_0';

INSERT INTO json_path_values_per_part_config VALUES (2, '{"value":true}');

SELECT arraySort(groupArray(id)) FROM json_path_values_per_part_config WHERE data.value = true
SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 1;
SELECT count() FROM json_path_values_per_part_config WHERE data.value = true
SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 1,
    query_plan_optimize_count_from_text_index = 1;

SYSTEM START MERGES json_path_values_per_part_config;
OPTIMIZE TABLE json_path_values_per_part_config FINAL;

SELECT arraySort(groupArray(id)) FROM json_path_values_per_part_config WHERE data.value = true
SETTINGS force_data_skipping_indices = 'idx', query_plan_direct_read_from_text_index = 1;

DROP TABLE json_path_values_per_part_config;
