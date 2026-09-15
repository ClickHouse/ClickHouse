DROP TABLE IF EXISTS json_bf_presence_native;
CREATE TABLE json_bf_presence_native (id UInt64, j JSON(max_dynamic_paths = 64, t String), INDEX bf j TYPE jsonbf_v1() GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO json_bf_presence_native VALUES
    (1, '{}'),
    (2, '{"x":null}'),
    (3, '{"x":""}'),
    (4, '{"x":0}'),
    (5, '{"x":[]}'),
    (6, '{"x":[null]}'),
    (7, '{"x":{}}'),
    (8, '{"x":{"a":1}}'),
    (9, '{"x":[{"a":1}]}'),
    (10, '{"x":true}'),
    (11, '{"x":"bad"}'),
    (12, '{"other":1}');

SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE isNotNull(j.x);
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE isNotNull(j.x) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE NOT isNotNull(j.x);
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE NOT isNotNull(j.x) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE isNotNull(j.missing);
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE isNotNull(j.missing) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE isNotNull(j.x) AND j.x.:String = '';
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE isNotNull(j.x) AND j.x.:String = '' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE isNotNull(j.x) OR j.other = 1;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE isNotNull(j.x) OR j.other = 1 SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE isNotNull(j.x.:String);
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE isNotNull(j.x.:String) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE isNotNull(j.t);
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE isNotNull(j.t) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE isNotNull(j.x.a);
SELECT arraySort(groupArray(id)) FROM json_bf_presence_native WHERE isNotNull(j.x.a) SETTINGS use_skip_indexes = 0;
SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM json_bf_presence_native WHERE isNotNull(j.missing)
    SETTINGS force_data_skipping_indices = 'bf', parallel_replicas_for_non_replicated_merge_tree = 0
)
WHERE trim(explain) = 'Granules: 0/12';
DROP TABLE json_bf_presence_native;

DROP TABLE IF EXISTS json_bf_presence_shared;
CREATE TABLE json_bf_presence_shared (id UInt64, j JSON(max_dynamic_paths = 0, t String), INDEX bf j TYPE jsonbf_v1() GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO json_bf_presence_shared VALUES
    (1, '{}'),
    (2, '{"x":null}'),
    (3, '{"x":""}'),
    (4, '{"x":0}'),
    (5, '{"x":[]}'),
    (6, '{"x":[null]}'),
    (7, '{"x":{}}'),
    (8, '{"x":{"a":1}}'),
    (9, '{"x":[{"a":1}]}'),
    (10, '{"x":true}'),
    (11, '{"x":"bad"}'),
    (12, '{"other":1}');

SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE isNotNull(j.x);
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE isNotNull(j.x) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE NOT isNotNull(j.x);
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE NOT isNotNull(j.x) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE isNotNull(j.missing);
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE isNotNull(j.missing) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE isNotNull(j.x) AND j.x.:String = '';
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE isNotNull(j.x) AND j.x.:String = '' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE isNotNull(j.x) OR j.other = 1;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE isNotNull(j.x) OR j.other = 1 SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE isNotNull(j.x.:String);
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE isNotNull(j.x.:String) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE isNotNull(j.t);
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE isNotNull(j.t) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE isNotNull(j.x.a);
SELECT arraySort(groupArray(id)) FROM json_bf_presence_shared WHERE isNotNull(j.x.a) SETTINGS use_skip_indexes = 0;
SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM json_bf_presence_shared WHERE isNotNull(j.missing)
    SETTINGS force_data_skipping_indices = 'bf', parallel_replicas_for_non_replicated_merge_tree = 0
)
WHERE trim(explain) = 'Granules: 0/12';
DROP TABLE json_bf_presence_shared;

-- Parts can retain an older path selection. Mixed null/non-null granules remain conservative after merging.
DROP TABLE IF EXISTS json_bf_presence_parts;
CREATE TABLE json_bf_presence_parts
(
    id UInt64,
    j JSON(max_dynamic_paths = 0),
    INDEX bf j TYPE jsonbf_v1(include_paths = ['other']) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1000000000;
SYSTEM STOP MERGES json_bf_presence_parts;
INSERT INTO json_bf_presence_parts VALUES (1, '{"x":"old","other":1}'), (2, '{}');
ALTER TABLE json_bf_presence_parts DETACH PARTITION tuple();
ALTER TABLE json_bf_presence_parts DROP INDEX bf;
ALTER TABLE json_bf_presence_parts ADD INDEX bf j TYPE jsonbf_v1(include_paths = ['x']) GRANULARITY 1;
ALTER TABLE json_bf_presence_parts ATTACH PARTITION tuple();
INSERT INTO json_bf_presence_parts VALUES (3, '{"x":"new"}'), (4, '{"x":null}');
SELECT arraySort(groupArray(id)) FROM json_bf_presence_parts WHERE isNotNull(j.x) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_presence_parts WHERE isNotNull(j.x) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_parts WHERE NOT isNotNull(j.x);
SELECT arraySort(groupArray(id)) FROM json_bf_presence_parts WHERE NOT isNotNull(j.x) SETTINGS use_skip_indexes = 0;
SYSTEM START MERGES json_bf_presence_parts;
OPTIMIZE TABLE json_bf_presence_parts FINAL;
SELECT arraySort(groupArray(id)) FROM json_bf_presence_parts WHERE isNotNull(j.x) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_presence_parts WHERE isNotNull(j.x) SETTINGS use_skip_indexes = 0;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 'json_bf_presence_parts' AND active;
DROP TABLE json_bf_presence_parts;
