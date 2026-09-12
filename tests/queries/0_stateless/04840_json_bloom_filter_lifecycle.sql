DROP TABLE IF EXISTS json_bf_lifecycle;

CREATE TABLE json_bf_lifecycle
(
    id UInt64,
    data JSON(
        marker String,
        nullable_items Array(Nullable(JSON(
            name String,
            price Int64)))),
    shared JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    payload String,
    INDEX data_idx data TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1,
    INDEX shared_idx shared TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 1,
    min_rows_for_wide_part = 1000000,
    min_bytes_for_wide_part = 1000000,
    enable_vertical_merge_algorithm = 0;

SYSTEM STOP MERGES json_bf_lifecycle;

INSERT INTO json_bf_lifecycle VALUES
    (1, '{"marker":"typed-one","nullable_items":[null,{"name":"kept","price":5}]}', '{"marker":"shared-one","nullable_items":[null,{"name":"shared-kept","price":6}]}', 'one'),
    (2, '{"marker":"typed-two","nullable_items":[{"name":"head","price":0},null,{"name":"tail","price":7}]}', '{"marker":"shared-two","nullable_items":[{"name":"shared-head","price":0},null]}', 'two');
INSERT INTO json_bf_lifecycle VALUES
    (3, '{"marker":"typed-three","nullable_items":[null]}', '{"marker":"shared-three","nullable_items":[null]}', 'three');
INSERT INTO json_bf_lifecycle VALUES
    (4, '{"marker":"typed-four","nullable_items":[]}', '{"marker":"shared-four","nullable_items":[]}', 'four');

SELECT 'compact parts before merge', count(), countIf(part_type = 'Compact')
FROM system.parts
WHERE database = currentDatabase() AND table = 'json_bf_lifecycle' AND active;
SELECT 'insert typed', groupArray(id) FROM json_bf_lifecycle WHERE data.marker = 'typed-one' SETTINGS force_data_skipping_indices = 'data_idx';
SELECT 'insert shared', groupArray(id) FROM json_bf_lifecycle WHERE shared.marker = 'shared-two' SETTINGS force_data_skipping_indices = 'shared_idx';

SYSTEM START MERGES json_bf_lifecycle;
OPTIMIZE TABLE json_bf_lifecycle FINAL;

SELECT 'compact parts after merge', count(), countIf(part_type = 'Compact')
FROM system.parts
WHERE database = currentDatabase() AND table = 'json_bf_lifecycle' AND active;
SELECT 'compact merge typed', groupArray(id) FROM json_bf_lifecycle WHERE data.marker = 'typed-three' SETTINGS force_data_skipping_indices = 'data_idx';
SELECT 'compact merge shared', groupArray(id) FROM json_bf_lifecycle WHERE shared.marker = 'shared-one' SETTINGS force_data_skipping_indices = 'shared_idx';

ALTER TABLE json_bf_lifecycle MODIFY SETTING
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    min_rows_for_full_part_storage = 0,
    min_bytes_for_full_part_storage = 0,
    min_level_for_full_part_storage = 0,
    enable_vertical_merge_algorithm = 1,
    allow_vertical_merges_from_compact_to_wide_parts = 1,
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 0;

SYSTEM STOP MERGES json_bf_lifecycle;
INSERT INTO json_bf_lifecycle VALUES
    (5, '{"marker":"typed-five","nullable_items":[null,{"name":"five","price":5}]}', '{"marker":"shared-five","nullable_items":[null,{"name":"shared-five","price":5}]}', 'five');
INSERT INTO json_bf_lifecycle VALUES
    (6, '{"marker":"typed-six","nullable_items":[{"name":"six","price":6},null]}', '{"marker":"shared-six","nullable_items":[{"name":"shared-six","price":6},null]}', 'six');
SYSTEM START MERGES json_bf_lifecycle;
OPTIMIZE TABLE json_bf_lifecycle FINAL;

SELECT 'wide parts after merge', count(), countIf(part_type = 'Wide')
FROM system.parts
WHERE database = currentDatabase() AND table = 'json_bf_lifecycle' AND active;
SELECT 'vertical merge typed', groupArray(id) FROM json_bf_lifecycle WHERE data.marker = 'typed-six' SETTINGS force_data_skipping_indices = 'data_idx';
SELECT 'vertical merge shared', groupArray(id) FROM json_bf_lifecycle WHERE shared.marker = 'shared-five' SETTINGS force_data_skipping_indices = 'shared_idx';

ALTER TABLE json_bf_lifecycle DROP INDEX data_idx;
ALTER TABLE json_bf_lifecycle DROP INDEX shared_idx;
ALTER TABLE json_bf_lifecycle ADD INDEX data_idx data TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1;
ALTER TABLE json_bf_lifecycle ADD INDEX shared_idx shared TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1;
ALTER TABLE json_bf_lifecycle MATERIALIZE INDEX data_idx SETTINGS mutations_sync = 2;
ALTER TABLE json_bf_lifecycle MATERIALIZE INDEX shared_idx SETTINGS mutations_sync = 2;

SELECT 'materialized typed', groupArray(id) FROM json_bf_lifecycle WHERE data.marker = 'typed-four' SETTINGS force_data_skipping_indices = 'data_idx';
SELECT 'materialized shared', groupArray(id) FROM json_bf_lifecycle WHERE shared.marker = 'shared-six' SETTINGS force_data_skipping_indices = 'shared_idx';

ALTER TABLE json_bf_lifecycle UPDATE
    data = '{"marker":"mutated-two","nullable_items":[null,{"name":"mutated","price":22}]}'::JSON,
    shared = '{"marker":"shared-mutated-two","nullable_items":[null,{"name":"shared-mutated","price":23}]}'::JSON
WHERE id = 2
SETTINGS mutations_sync = 2;

SELECT 'mutation typed', groupArray(id) FROM json_bf_lifecycle WHERE data.marker = 'mutated-two' SETTINGS force_data_skipping_indices = 'data_idx';
SELECT 'mutation shared', groupArray(id) FROM json_bf_lifecycle WHERE shared.marker = 'shared-mutated-two' SETTINGS force_data_skipping_indices = 'shared_idx';

DETACH TABLE json_bf_lifecycle;
ATTACH TABLE json_bf_lifecycle;

SELECT 'attached typed', groupArray(id) FROM json_bf_lifecycle WHERE data.marker = 'mutated-two' SETTINGS force_data_skipping_indices = 'data_idx';
SELECT 'attached shared', groupArray(id) FROM json_bf_lifecycle WHERE shared.marker = 'shared-mutated-two' SETTINGS force_data_skipping_indices = 'shared_idx';

DROP TABLE json_bf_lifecycle;
