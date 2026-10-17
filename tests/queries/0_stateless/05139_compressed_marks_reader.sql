-- Tags: no-random-merge-tree-settings
-- Keep marks split across blocks while exercising codecs, layouts, and concurrent readers.
CREATE TABLE marks_reader_wide (k UInt32, v UInt64, lc LowCardinality(String), arr Array(UInt64), n Nullable(Int64))
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 16, marks_compress_block_size = 17,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, compress_marks = 1, prewarm_mark_cache = 0;
CREATE TABLE marks_reader_lz4 AS marks_reader_wide;
ALTER TABLE marks_reader_lz4 MODIFY SETTING marks_compress_block_size = 23, marks_compression_codec = 'LZ4';
CREATE TABLE marks_reader_none AS marks_reader_wide;
ALTER TABLE marks_reader_none MODIFY SETTING marks_compress_block_size = 31, marks_compression_codec = 'NONE';
CREATE TABLE marks_reader_fixed AS marks_reader_wide ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 16, index_granularity_bytes = 0, enable_mixed_granularity_parts = 0,
    marks_compress_block_size = 17, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    compress_marks = 1, prewarm_mark_cache = 0;
CREATE TABLE marks_reader_plain AS marks_reader_wide;
ALTER TABLE marks_reader_plain MODIFY SETTING compress_marks = 0;
CREATE TABLE marks_reader_compact AS marks_reader_wide;
ALTER TABLE marks_reader_compact MODIFY SETTING min_bytes_for_wide_part = 1000000000,
    min_rows_for_wide_part = 1000000000, write_marks_for_substreams_in_compact_parts = 0;
CREATE TABLE marks_reader_substreams AS marks_reader_compact;
ALTER TABLE marks_reader_substreams MODIFY SETTING write_marks_for_substreams_in_compact_parts = 1;

INSERT INTO marks_reader_wide SELECT number, number * 10, toString(number % 16),
    arrayMap(x -> number + x, range(number % 3)), if(number % 7 = 0, NULL, toInt64(number) - 1000) FROM numbers(2048);
INSERT INTO marks_reader_lz4 SELECT * FROM marks_reader_wide;
INSERT INTO marks_reader_none SELECT * FROM marks_reader_wide;
INSERT INTO marks_reader_fixed SELECT * FROM marks_reader_wide;
INSERT INTO marks_reader_plain SELECT * FROM marks_reader_wide;
INSERT INTO marks_reader_compact SELECT * FROM marks_reader_wide;
INSERT INTO marks_reader_substreams SELECT * FROM marks_reader_wide;
DETACH TABLE marks_reader_wide;
ATTACH TABLE marks_reader_wide;

SELECT count(), arraySort(groupUniqArray((c, v, arr, n, lc))) FROM
(
    SELECT count() AS c, sum(v) AS v, sum(arraySum(arr)) AS arr, sum(n) AS n, sum(cityHash64(lc)) AS lc
    FROM merge(currentDatabase(), '^marks_reader_') WHERE k BETWEEN 1001 AND 1042 GROUP BY _table
) SETTINGS load_marks_asynchronously = 0;
SELECT count(), arraySort(groupUniqArray(rows)) FROM
(
    SELECT groupArray((k, v, lc, arr, n)) AS rows FROM
        (SELECT _table, k, v, lc, arr, n FROM merge(currentDatabase(), '^marks_reader_')
            WHERE k IN (0, 15, 16, 17, 2047) ORDER BY _table, k)
    GROUP BY _table
) SETTINGS load_marks_asynchronously = 1;
SELECT count(), arraySort(groupUniqArray((c, v, arr, n, lc))) FROM
(
    SELECT count() AS c, sum(v) AS v, sum(arraySum(arr)) AS arr, sum(n) AS n, sum(cityHash64(lc)) AS lc
    FROM merge(currentDatabase(), '^marks_reader_') WHERE k % 17 = 0 GROUP BY _table
) SETTINGS max_threads = 8, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1;

DROP TABLE marks_reader_wide, marks_reader_lz4, marks_reader_none, marks_reader_fixed,
    marks_reader_plain, marks_reader_compact, marks_reader_substreams;
