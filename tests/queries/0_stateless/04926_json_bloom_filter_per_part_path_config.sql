SET enable_json_type = 1;

DROP TABLE IF EXISTS json_bf_per_part_config;

CREATE TABLE json_bf_per_part_config
(
    id UInt64,
    data JSON(max_dynamic_paths = 0, old_path String, new_path String),
    INDEX idx data TYPE jsonbf_v1(
        false_positive_rate = 0.0001,
        include_paths = ['old_path'],
        skip_paths = ['new_path']) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_per_part_config VALUES (1, '{"old_path":"old","new_path":"old-hit"}');

ALTER TABLE json_bf_per_part_config DETACH PARTITION tuple();
ALTER TABLE json_bf_per_part_config DROP INDEX idx;
ALTER TABLE json_bf_per_part_config ADD INDEX idx data TYPE jsonbf_v1(
    false_positive_rate = 0.0001,
    include_paths = ['new_path'],
    skip_paths = ['old_path']) GRANULARITY 1;
ALTER TABLE json_bf_per_part_config ATTACH PARTITION tuple();

INSERT INTO json_bf_per_part_config VALUES (2, '{"old_path":"new","new_path":"new-hit"}');

SELECT groupArray(id) FROM json_bf_per_part_config WHERE data.new_path = 'old-hit'
SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_per_part_config WHERE data.new_path = 'new-hit'
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_bf_per_part_config WHERE data.new_path = 'missing'
SETTINGS force_data_skipping_indices = 'idx';

OPTIMIZE TABLE json_bf_per_part_config FINAL;

SELECT groupArray(id) FROM json_bf_per_part_config WHERE data.new_path = 'old-hit'
SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(id) FROM json_bf_per_part_config WHERE data.new_path = 'new-hit'
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_bf_per_part_config WHERE data.new_path = 'missing'
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_bf_per_part_config;
