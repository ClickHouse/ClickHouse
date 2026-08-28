SET enable_json_type = 1;
SET mutations_sync = 2, alter_sync = 2;

DROP TABLE IF EXISTS json_path_values_per_part_config;

CREATE TABLE json_path_values_per_part_config
(
    id UInt64,
    data JSON(max_dynamic_paths = 0, old_path String, new_path String),
    INDEX idx data TYPE text(tokenizer = jsonPathValues(
        max_token_bytes = 64,
        include_paths = ['old_path'],
        skip_paths = ['new_path'])) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_per_part_config VALUES (1, '{"old_path":"old","new_path":"old-hit"}');

SYSTEM STOP MERGES json_path_values_per_part_config;
ALTER TABLE json_path_values_per_part_config DETACH PART 'all_1_1_0';
ALTER TABLE json_path_values_per_part_config DROP INDEX idx;
ALTER TABLE json_path_values_per_part_config ADD INDEX idx data TYPE text(tokenizer = jsonPathValues(
    max_token_bytes = 64,
    include_paths = ['new_path'],
    skip_paths = ['old_path'])) GRANULARITY 1;
ALTER TABLE json_path_values_per_part_config ATTACH PART 'all_1_1_0';

INSERT INTO json_path_values_per_part_config VALUES (2, '{"old_path":"new","new_path":"new-hit"}');

SELECT arraySort(groupArray((has_old_path, has_new_path)))
FROM
(
    SELECT
        countIf(startsWith(hex(token), concat(hex('old_path'), '0000'))) > 0 AS has_old_path,
        countIf(startsWith(hex(token), concat(hex('new_path'), '0000'))) > 0 AS has_new_path
    FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_per_part_config', 'idx')
    GROUP BY part_name
);

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

SYSTEM START MERGES json_path_values_per_part_config;
DROP TABLE json_path_values_per_part_config;
