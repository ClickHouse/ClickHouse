-- A column whose every value has expired by its TTL is dropped by a merge without reading it. When that is
-- all a merge of a single part has to do, the files of the other columns are hardlinked instead of rewritten.
-- With `ttl_only_drop_parts`, a column TTL is applied only by dropping such fully expired columns.
-- Background TTL merges are disabled (`max_number_of_merges_with_ttl_in_pool = 0`), `OPTIMIZE` merges instead.

SET optimize_throw_if_noop = 1;

DROP TABLE IF EXISTS t_ttl_hardlink;

CREATE TABLE t_ttl_hardlink
(
    d Date,
    key UInt64,
    value String,
    props JSON TTL d + INTERVAL 1 DAY,
    INDEX props_paths JSONAllPaths(props) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ttl_only_drop_parts = 1, max_number_of_merges_with_ttl_in_pool = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0;

INSERT INTO t_ttl_hardlink SELECT '2020-01-01', number, toString(number), '{"a" : 1}' FROM numbers(100);

SELECT 'single part, fully expired';
OPTIMIZE TABLE t_ttl_hardlink FINAL;
SELECT count(), sum(key), countIf(props::String = '{}') FROM t_ttl_hardlink;
SELECT count() FROM t_ttl_hardlink WHERE has(JSONAllPaths(props), 'a');
SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ttl_hardlink' AND active AND column = 'props';
CHECK TABLE t_ttl_hardlink SETTINGS check_query_single_value_result = 1;

SYSTEM FLUSH LOGS part_log;
SELECT merge_algorithm, ProfileEvents['MutationSomePartColumns'], ProfileEvents['MutationAllPartColumns']
FROM system.part_log WHERE database = currentDatabase() AND table = 't_ttl_hardlink' AND event_type = 'MergeParts';

DROP TABLE t_ttl_hardlink;

-- Several parts: the column is dropped by a regular merge, which stays vertical.
-- A part whose column has not fully expired keeps it with `ttl_only_drop_parts = 1` and clears the expired values with `0`.
DROP TABLE IF EXISTS t_ttl_multi_0;
DROP TABLE IF EXISTS t_ttl_multi_1;

CREATE TABLE t_ttl_multi_0
(
    d Date,
    key UInt64,
    props JSON TTL d + INTERVAL 1 DAY
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ttl_only_drop_parts = 0, max_number_of_merges_with_ttl_in_pool = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0;

CREATE TABLE t_ttl_multi_1 AS t_ttl_multi_0;
ALTER TABLE t_ttl_multi_1 MODIFY SETTING ttl_only_drop_parts = 1;

SYSTEM STOP MERGES t_ttl_multi_0;
SYSTEM STOP MERGES t_ttl_multi_1;

INSERT INTO t_ttl_multi_0 VALUES ('2020-01-01', 1, '{"a" : 1}');
INSERT INTO t_ttl_multi_0 VALUES ('2020-01-01', 2, '{"a" : 2}');
INSERT INTO t_ttl_multi_1 SELECT * FROM t_ttl_multi_0 WHERE key = 1;
INSERT INTO t_ttl_multi_1 SELECT * FROM t_ttl_multi_0 WHERE key = 2;

SYSTEM START MERGES t_ttl_multi_0;
SYSTEM START MERGES t_ttl_multi_1;

SELECT 'several parts, fully expired';
OPTIMIZE TABLE t_ttl_multi_0 FINAL;
OPTIMIZE TABLE t_ttl_multi_1 FINAL;
SELECT key, props FROM t_ttl_multi_0 ORDER BY key;
SELECT key, props FROM t_ttl_multi_1 ORDER BY key;

SYSTEM STOP MERGES t_ttl_multi_0;
SYSTEM STOP MERGES t_ttl_multi_1;

INSERT INTO t_ttl_multi_0 VALUES ('2020-01-01', 3, '{"a" : 3}'), ('2100-01-01', 4, '{"a" : 4}');
INSERT INTO t_ttl_multi_1 VALUES ('2020-01-01', 3, '{"a" : 3}'), ('2100-01-01', 4, '{"a" : 4}');

SYSTEM START MERGES t_ttl_multi_0;
SYSTEM START MERGES t_ttl_multi_1;

SELECT 'partially expired';
OPTIMIZE TABLE t_ttl_multi_0 FINAL;
OPTIMIZE TABLE t_ttl_multi_1 FINAL;
SELECT key, props FROM t_ttl_multi_0 ORDER BY key;
SELECT key, props FROM t_ttl_multi_1 ORDER BY key;

SYSTEM FLUSH LOGS part_log;
SELECT table, merge_algorithm, ProfileEvents['MutationSomePartColumns']
FROM system.part_log WHERE database = currentDatabase() AND table LIKE 't_ttl_multi_%' AND event_type = 'MergeParts'
ORDER BY table, event_time_microseconds;

DROP TABLE t_ttl_multi_0;
DROP TABLE t_ttl_multi_1;

-- A part of `ReplacingMergeTree` that has not been merged yet may contain rows to replace, so the merge must read it.
DROP TABLE IF EXISTS t_ttl_replacing;

CREATE TABLE t_ttl_replacing
(
    d Date,
    key UInt64,
    props JSON TTL d + INTERVAL 1 DAY
)
ENGINE = ReplacingMergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ttl_only_drop_parts = 1, max_number_of_merges_with_ttl_in_pool = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0;

INSERT INTO t_ttl_replacing SETTINGS optimize_on_insert = 0 VALUES ('2020-01-01', 1, '{"a" : 1}'), ('2020-01-01', 1, '{"a" : 2}');

SELECT 'replacing';
OPTIMIZE TABLE t_ttl_replacing FINAL;
SELECT key, props FROM t_ttl_replacing;

SYSTEM FLUSH LOGS part_log;
SELECT merge_algorithm, ProfileEvents['MutationSomePartColumns']
FROM system.part_log WHERE database = currentDatabase() AND table = 't_ttl_replacing' AND event_type = 'MergeParts';

DROP TABLE t_ttl_replacing;

-- A `RECOMPRESS` TTL that is due must still recompress the part, so the files cannot be hardlinked.
DROP TABLE IF EXISTS t_ttl_recompress;

CREATE TABLE t_ttl_recompress
(
    d Date,
    key UInt64,
    props JSON TTL d + INTERVAL 1 DAY
)
ENGINE = MergeTree ORDER BY key
TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(1))
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ttl_only_drop_parts = 1, max_number_of_merges_with_ttl_in_pool = 0;

INSERT INTO t_ttl_recompress VALUES ('2020-01-01', 1, '{"a" : 1}');

SELECT 'recompress';
OPTIMIZE TABLE t_ttl_recompress FINAL;
SELECT key, props FROM t_ttl_recompress;
SELECT default_compression_codec FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_recompress' AND active;

SYSTEM FLUSH LOGS part_log;
SELECT ProfileEvents['MutationSomePartColumns']
FROM system.part_log WHERE database = currentDatabase() AND table = 't_ttl_recompress' AND event_type = 'MergeParts';

DROP TABLE t_ttl_recompress;
