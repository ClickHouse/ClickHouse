-- Tags: no-random-settings, no-random-merge-tree-settings

-- Compact selection and fail-closed gates for with_key_columns.
-- optimize_on_insert must stay off: it writes level-1 parts, which Compact
-- support must force to Wide.

SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_wkc_compact_gates;
DROP TABLE IF EXISTS t_wkc_compact_empty;
DROP TABLE IF EXISTS t_wkc_compact_thresh;

SELECT 'compact part type';
CREATE TABLE t_wkc_compact_gates
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000;

INSERT INTO t_wkc_compact_gates VALUES (1, {'a': 1, 'b': 2});
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_wkc_compact_gates' AND active;
SELECT id, m, m['a'], m['b'], m['missing'], m.exists_a, m.exists_missing FROM t_wkc_compact_gates ORDER BY id;
CHECK TABLE t_wkc_compact_gates SETTINGS check_query_single_value_result = 1;

SELECT 'empty map compact';
CREATE TABLE t_wkc_compact_empty
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000;

INSERT INTO t_wkc_compact_empty VALUES (1, map());
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_wkc_compact_empty' AND active;
SELECT id, m, m['a'], m.exists_a FROM t_wkc_compact_empty;
SELECT
    has(substreams, 'm.keys_info'),
    has(substreams, 'm.key_a'),
    has(substreams, 'm.key_presence')
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_wkc_compact_empty' AND column = 'm' AND active;

SELECT 'optimize final -> wide';
OPTIMIZE TABLE t_wkc_compact_gates FINAL;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_wkc_compact_gates' AND active;
SELECT id, m, m['a'], m['b'] FROM t_wkc_compact_gates ORDER BY id;

SELECT 'byte threshold forces wide';
CREATE TABLE t_wkc_compact_thresh
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    max_bytes_for_compact_map_key_columns = 0;

INSERT INTO t_wkc_compact_thresh VALUES (1, {'a': 1});
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_wkc_compact_thresh' AND active;

SELECT 'write marks required';
CREATE TABLE t_wkc_compact_nomarks
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    write_marks_for_substreams_in_compact_parts = 0; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE t_wkc_compact_gates;
DROP TABLE t_wkc_compact_empty;
DROP TABLE t_wkc_compact_thresh;
