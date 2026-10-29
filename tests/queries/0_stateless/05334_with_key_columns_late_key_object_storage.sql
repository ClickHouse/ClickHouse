-- Tags: no-fasttest, no-random-merge-tree-settings
SET allow_experimental_map_key_columns = 1;
-- Tag no-fasttest: needs S3
-- Tag no-random-merge-tree-settings: the randomized `map_serialization_version` replaces `with_key_columns`

-- A key that first appears after some granules of the part were written is supported on object storage.

DROP TABLE IF EXISTS t_wkc_late_key_s3;

CREATE TABLE t_wkc_late_key_s3 (id UInt64, m Map(String, UInt64), lc Map(String, LowCardinality(String)))
ENGINE = MergeTree ORDER BY id
SETTINGS
    storage_policy = 's3_cache',
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity = 1000,
    index_granularity_bytes = '10Mi';

INSERT INTO t_wkc_late_key_s3 SELECT number, map('a', number), map('a', 'x') FROM numbers(30000);

ALTER TABLE t_wkc_late_key_s3 UPDATE m = map('a', id, 'late', id), lc = map('a', 'x', 'late', 'y') WHERE id >= 20500
SETTINGS mutations_sync = 2;

SELECT count(), countIf(mapContains(m, 'late')), sum(m['late']), countIf(lc['late'] = 'y'), countIf(lc['late'] = '') FROM t_wkc_late_key_s3;
SELECT countIf(m = if(id >= 20500, map('a', id, 'late', id), map('a', id))) FROM t_wkc_late_key_s3;
SELECT id, m, lc FROM t_wkc_late_key_s3 WHERE id IN (20499, 20500) ORDER BY id;

CHECK TABLE t_wkc_late_key_s3 SETTINGS check_query_single_value_result = 1;

DROP TABLE t_wkc_late_key_s3;
