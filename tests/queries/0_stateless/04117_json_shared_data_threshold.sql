-- Test: `object_shared_data_min_bytes_for_advanced_serialization` keeps every
-- dynamic path of a JSON column inside the single shared-data substream while
-- the part stays in `Wide` format. This avoids the per-dynamic-path file
-- fan-out that otherwise turns into one S3 `PUT` per file on small parts
-- (https://github.com/ClickHouse/ClickHouse/issues/100960,
--  https://github.com/ClickHouse/ClickHouse/issues/102052).

SET allow_experimental_json_type = 1;

DROP TABLE IF EXISTS t_json_shared_threshold;

-- Threshold of 1 GiB — far above the test part size, so the gate is active.
CREATE TABLE t_json_shared_threshold
(
    id UInt64,
    s  String,
    j  JSON(max_dynamic_paths = 100)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    object_shared_data_buckets_for_wide_part = 4,
    object_shared_data_min_bytes_for_advanced_serialization = 1073741824;

-- 50 distinct paths — without the gate, each promoted dynamic path would get
-- its own `.bin` substream, but the threshold is set so high that we expect
-- all of them to live inside the shared-data substream instead.
INSERT INTO t_json_shared_threshold
SELECT
    number,
    'row_' || toString(number),
    '{"path_' || toString(number % 50) || '": ' || toString(number) || '}'
FROM numbers(200);

SELECT '-- Below threshold: part stays Wide';
SELECT part_type
FROM system.parts
WHERE database = currentDatabase() AND table = 't_json_shared_threshold' AND active = 1
LIMIT 1;

-- A per-dynamic-path substream is named `j.<path>.dynamic_data` (the leaf of
-- `SerializationDynamic` under each `Substream::ObjectDynamicPath`). With the
-- gate active, none of these substreams should exist for the JSON column.
SELECT '-- Below threshold: zero per-dynamic-path substreams';
SELECT length(arrayFilter(x -> x LIKE 'j.path\_%' AND position(x, 'object_shared_data') = 0, substreams))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_shared_threshold' AND column = 'j' AND active = 1
LIMIT 1;

SELECT '-- Below threshold: data is readable';
SELECT count(), countIf(j.path_0 IS NOT NULL) FROM t_json_shared_threshold;

DROP TABLE t_json_shared_threshold;

-- Same data, threshold disabled: the per-dynamic-path substreams DO appear.
DROP TABLE IF EXISTS t_json_no_threshold;

CREATE TABLE t_json_no_threshold
(
    id UInt64,
    s  String,
    j  JSON(max_dynamic_paths = 100)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    object_shared_data_buckets_for_wide_part = 4,
    object_shared_data_min_bytes_for_advanced_serialization = 0;

INSERT INTO t_json_no_threshold
SELECT
    number,
    'row_' || toString(number),
    '{"path_' || toString(number % 50) || '": ' || toString(number) || '}'
FROM numbers(200);

SELECT '-- Threshold disabled: per-dynamic-path substreams ARE present';
SELECT length(arrayFilter(x -> x LIKE 'j.path\_%' AND position(x, 'object_shared_data') = 0, substreams)) > 0
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_no_threshold' AND column = 'j' AND active = 1
LIMIT 1;

DROP TABLE t_json_no_threshold;

-- A table without any JSON / Object / Dynamic column is unaffected by the
-- new setting — the part stays Wide and we don't go through the JSON write
-- path at all.
DROP TABLE IF EXISTS t_no_json;

CREATE TABLE t_no_json (id UInt64, s String)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    object_shared_data_min_bytes_for_advanced_serialization = 1073741824;

INSERT INTO t_no_json SELECT number, 'val_' || toString(number) FROM numbers(200);

SELECT '-- Setting has no effect on tables without dynamic-subcolumn columns';
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_no_json' AND active = 1 LIMIT 1;
SELECT count() FROM t_no_json;

DROP TABLE t_no_json;

-- The gate is intentionally Wide-only: in Compact parts every column is packed
-- into a single `.bin` file so the per-dynamic-path file fan-out problem does
-- not exist there, and Compact uses `StatisticsMode::PREFIX` which would
-- record stale path statistics if folding ran. Verify the data is readable
-- and unaffected by the setting.
DROP TABLE IF EXISTS t_json_compact_threshold;

CREATE TABLE t_json_compact_threshold
(
    id UInt64,
    j  JSON(max_dynamic_paths = 100)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = '200G',
    min_rows_for_wide_part = 1000000,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    object_shared_data_min_bytes_for_advanced_serialization = 1073741824;

INSERT INTO t_json_compact_threshold
SELECT number, '{"path_' || toString(number % 50) || '": ' || toString(number) || '}'
FROM numbers(200);

SELECT '-- Setting does not change Compact part type';
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_json_compact_threshold' AND active = 1 LIMIT 1;

SELECT '-- Compact part data is readable';
SELECT count(), countIf(j.path_0 IS NOT NULL) FROM t_json_compact_threshold;

DROP TABLE t_json_compact_threshold;
