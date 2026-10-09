-- Rows may hold keys outside the source parts' key sets: a column added by ALTER is
-- evaluated from its DEFAULT, and mutations compute new values.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_unknown_keys;
CREATE TABLE t_unknown_keys (id UInt64, part_type String) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types', propagate_types_serialization_versions_to_nested_types = 1, min_bytes_for_wide_part = 0, index_granularity = 100, merge_max_block_size = 100,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

-- `ALTER ADD COLUMN` with DEFAULT, then merges (Wide and Compact, horizontal and vertical).
SELECT 'add column';
INSERT INTO t_unknown_keys SELECT number, 'w' FROM numbers(150);
INSERT INTO t_unknown_keys SELECT number + 150, 'w' FROM numbers(150);
ALTER TABLE t_unknown_keys ADD COLUMN m Map(String, UInt64) DEFAULT map(concat('k', toString(intDiv(id, 50))), id);
ALTER TABLE t_unknown_keys ADD COLUMN am Array(Map(String, UInt64)) DEFAULT [map(concat('a', toString(intDiv(id, 50))), id)];
INSERT INTO t_unknown_keys (id, part_type, m, am) VALUES (5000, 'w', {'x': 1}, [{'y': 2}]);
OPTIMIZE TABLE t_unknown_keys FINAL;
SELECT count(), sum(length(m)), uniqExact(arrayJoin(mapKeys(m))), sum(m[concat('k', toString(intDiv(id, 50)))]), sum(am[1][concat('a', toString(intDiv(id, 50)))]) FROM t_unknown_keys;

ALTER TABLE t_unknown_keys MODIFY SETTING vertical_merge_algorithm_min_rows_to_activate = 1000000;
ALTER TABLE t_unknown_keys ADD COLUMN m2 Map(String, UInt64) DEFAULT map(concat('m', toString(intDiv(id, 50))), id);
INSERT INTO t_unknown_keys (id, part_type) VALUES (6000, 'w');
OPTIMIZE TABLE t_unknown_keys FINAL;
SELECT count(), sum(length(m2)), sum(m2[concat('m', toString(intDiv(id, 50)))]) FROM t_unknown_keys;

-- `MODIFY COLUMN ... MATERIALIZED`, then `MATERIALIZE COLUMN`.
SELECT 'materialize column';
ALTER TABLE t_unknown_keys MODIFY COLUMN m Map(String, UInt64) MATERIALIZED map(concat('z', toString(intDiv(id, 50))), id);
ALTER TABLE t_unknown_keys MATERIALIZE COLUMN m;
ALTER TABLE t_unknown_keys MODIFY COLUMN am Array(Map(String, UInt64)) MATERIALIZED [map(concat('w', toString(intDiv(id, 50))), id)];
ALTER TABLE t_unknown_keys MATERIALIZE COLUMN am;
SELECT count(), sum(length(m)), sum(m[concat('z', toString(intDiv(id, 50)))]), sum(am[1][concat('w', toString(intDiv(id, 50)))]) FROM t_unknown_keys;

-- `UPDATE`.
SELECT 'update';
ALTER TABLE t_unknown_keys UPDATE m2 = map(concat('u', toString(intDiv(id, 50))), id) WHERE 1;
SELECT count(), sum(length(m2)), sum(m2[concat('u', toString(intDiv(id, 50)))]) FROM t_unknown_keys;
DROP TABLE t_unknown_keys;

SELECT 'compact';
DROP TABLE IF EXISTS t_unknown_keys_compact;
CREATE TABLE t_unknown_keys_compact (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types', propagate_types_serialization_versions_to_nested_types = 1, min_bytes_for_wide_part = '10G', index_granularity = 100, merge_max_block_size = 100,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;
INSERT INTO t_unknown_keys_compact SELECT number FROM numbers(150);
INSERT INTO t_unknown_keys_compact SELECT number + 150 FROM numbers(150);
ALTER TABLE t_unknown_keys_compact ADD COLUMN m Map(String, UInt64) DEFAULT map(concat('k', toString(intDiv(id, 50))), id);
ALTER TABLE t_unknown_keys_compact ADD COLUMN am Array(Map(String, UInt64)) DEFAULT [map(concat('a', toString(intDiv(id, 50))), id)];
INSERT INTO t_unknown_keys_compact (id, m, am) VALUES (5000, {'x': 1}, [{'y': 2}]);
OPTIMIZE TABLE t_unknown_keys_compact FINAL;
SELECT count(), sum(length(m)), sum(m[concat('k', toString(intDiv(id, 50)))]), sum(am[1][concat('a', toString(intDiv(id, 50)))]) FROM t_unknown_keys_compact;
ALTER TABLE t_unknown_keys_compact MODIFY COLUMN m Map(String, UInt64) MATERIALIZED map(concat('z', toString(intDiv(id, 50))), id);
ALTER TABLE t_unknown_keys_compact MATERIALIZE COLUMN m;
ALTER TABLE t_unknown_keys_compact MODIFY COLUMN am Array(Map(String, UInt64)) MATERIALIZED [map(concat('w', toString(intDiv(id, 50))), id)];
ALTER TABLE t_unknown_keys_compact MATERIALIZE COLUMN am;
SELECT count(), sum(length(m)), sum(m[concat('z', toString(intDiv(id, 50)))]), sum(am[1][concat('w', toString(intDiv(id, 50)))]) FROM t_unknown_keys_compact;
DROP TABLE t_unknown_keys_compact;
