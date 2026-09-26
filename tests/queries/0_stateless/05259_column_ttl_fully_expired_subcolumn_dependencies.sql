-- A merge drops a column whose every value has expired by its TTL without reading it. A skip index that depends on
-- a subcolumn of such a column (here the typed path `props.a` of a `JSON` column) must not be taken from the source
-- parts, which describe the values before they expired: readers see the default value of the column.
-- (A projection cannot depend on a subcolumn without storing the whole column, so it is not covered here.)
-- A multi-column skip index that depends on the column is built from its default values.
-- Background TTL merges are disabled (`max_number_of_merges_with_ttl_in_pool = 0`), `OPTIMIZE` merges instead.

SET optimize_throw_if_noop = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_ttl_subcolumn_deps;

CREATE TABLE t_ttl_subcolumn_deps
(
    d Date,
    key UInt64,
    s String TTL d + INTERVAL 1 DAY,
    props JSON(a UInt64) TTL d + INTERVAL 1 DAY,
    INDEX idx_a props.a TYPE minmax GRANULARITY 1,
    INDEX idx_key_s (key, s) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ttl_only_drop_parts = 1, max_number_of_merges_with_ttl_in_pool = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0;

-- A single part: the other columns are hardlinked.
INSERT INTO t_ttl_subcolumn_deps SELECT '2020-01-01', number, 'foo', '{"a" : 1}' FROM numbers(100);

SELECT 'single part';
OPTIMIZE TABLE t_ttl_subcolumn_deps FINAL;
SELECT count(), countIf(s = ''), countIf(props.a = 0) FROM t_ttl_subcolumn_deps SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ttl_subcolumn_deps WHERE props.a = 0 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_ttl_subcolumn_deps WHERE key < 1000 AND s = '' SETTINGS use_skip_indexes = 1;
CHECK TABLE t_ttl_subcolumn_deps SETTINGS check_query_single_value_result = 1;

-- Several parts: the column is dropped by a regular merge.
DROP TABLE IF EXISTS t_ttl_subcolumn_deps_multi;
CREATE TABLE t_ttl_subcolumn_deps_multi AS t_ttl_subcolumn_deps;
DROP TABLE t_ttl_subcolumn_deps;

SYSTEM STOP MERGES t_ttl_subcolumn_deps_multi;
INSERT INTO t_ttl_subcolumn_deps_multi SELECT '2020-01-01', number, 'foo', '{"a" : 1}' FROM numbers(100);
INSERT INTO t_ttl_subcolumn_deps_multi SELECT '2020-01-01', number + 100, 'foo', '{"a" : 1}' FROM numbers(100);
SYSTEM START MERGES t_ttl_subcolumn_deps_multi;

SELECT 'several parts';
OPTIMIZE TABLE t_ttl_subcolumn_deps_multi FINAL;
SELECT count(), countIf(s = ''), countIf(props.a = 0) FROM t_ttl_subcolumn_deps_multi SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ttl_subcolumn_deps_multi WHERE props.a = 0 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_ttl_subcolumn_deps_multi WHERE key < 1000 AND s = '' SETTINGS use_skip_indexes = 1;
CHECK TABLE t_ttl_subcolumn_deps_multi SETTINGS check_query_single_value_result = 1;

DROP TABLE t_ttl_subcolumn_deps_multi;
