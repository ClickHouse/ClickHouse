-- Tags: no-random-merge-tree-settings
-- The packed-skip-index archive stores virtual file names as raw byte strings (no JSON / UTF-8
-- validation in PackedFilesIO), so the writer must be able to put arbitrary index names into
-- the archive and the reader must round-trip them back without corruption. Exercise:
--   * long index names that would normally trigger replace_long_file_name_to_hash on disk
--     (virtual files inside the archive aren't subject to filesystem name limits, so the
--     packed writer keeps the unhashed form),
--   * an index name containing NULL byte / invalid UTF-8 / Cyrillic chars.
-- Both DETACH/ATTACH (re-loading the part) and CHECK TABLE must succeed in both cases.

-- Case A: long index name with hashing enabled.
DROP TABLE IF EXISTS t_packed_long_idx;
CREATE TABLE t_packed_long_idx
(
    id Int64,
    v Int64,
    INDEX `explicit_index_with_an_extremely_long_name_that_exceeds_the_maximum_file_name_length` v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    replace_long_file_name_to_hash = 1,
    max_file_name_length = 50,
    min_bytes_for_wide_part = 0,
    index_granularity = 2,
    add_minmax_index_for_numeric_columns = 0,
    packed_skip_index_max_bytes = 4194304;

INSERT INTO t_packed_long_idx VALUES
    (1, 100), (2, 200), (3, 300), (4, 400),
    (5, 500), (6, 600), (7, 700), (8, 800);

SELECT 'long_count', count() FROM t_packed_long_idx;
SELECT 'long_v_eq_100', count() FROM t_packed_long_idx WHERE v = 100;
CHECK TABLE t_packed_long_idx SETTINGS check_query_single_value_result = 1;

DETACH TABLE t_packed_long_idx;
ATTACH TABLE t_packed_long_idx;

SELECT 'long_count_after_attach', count() FROM t_packed_long_idx;
SELECT 'long_v_eq_500_after_attach', count() FROM t_packed_long_idx WHERE v = 500;
CHECK TABLE t_packed_long_idx SETTINGS check_query_single_value_result = 1;

DROP TABLE t_packed_long_idx;

-- Case B: invalid UTF-8 / NULL byte / Cyrillic in the index name. The escape_index_filenames
-- default (true since 26.1) converts these to %XX-escaped ASCII for the on-disk and in-archive
-- names, so the resulting virtual file name is plain ASCII even though the metadata index name
-- itself is binary.
DROP TABLE IF EXISTS t_packed_bad_utf8;
CREATE TABLE t_packed_bad_utf8
(
    v UInt8,
    INDEX `minmax_idx_\xFF\0привет` v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    add_minmax_index_for_numeric_columns = 0,
    min_bytes_for_wide_part = 0,
    packed_skip_index_max_bytes = 4194304;

INSERT INTO t_packed_bad_utf8 SELECT number FROM numbers(1);
INSERT INTO t_packed_bad_utf8 SELECT number FROM numbers(10000);

SELECT 'bad_utf8_count', count() FROM t_packed_bad_utf8;
SELECT 'bad_utf8_range', min(v), max(v) FROM t_packed_bad_utf8;
CHECK TABLE t_packed_bad_utf8 SETTINGS check_query_single_value_result = 1;

DETACH TABLE t_packed_bad_utf8;
ATTACH TABLE t_packed_bad_utf8;

SELECT 'bad_utf8_count_after_attach', count() FROM t_packed_bad_utf8;
CHECK TABLE t_packed_bad_utf8 SETTINGS check_query_single_value_result = 1;

DROP TABLE t_packed_bad_utf8;
