-- Tags: no-fasttest
-- no-fasttest: Parquet support is not built in fast-test images

-- A single Parquet file is read by several sources, each reading a subset of its row groups.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05394.parquet')
SELECT number AS id, toString(number % 1000) AS s, [number, number + 1] AS arr, (number, toString(number)) AS t
FROM numbers(100000)
SETTINGS output_format_parquet_row_group_size = 1000, output_format_parquet_use_custom_encoder = 1;

-- The file is split between the streams, one source per stream.
SELECT trimLeft(explain) FROM (
    EXPLAIN PIPELINE SELECT sum(id) FROM file(currentDatabase() || '_05394.parquet')
    SETTINGS max_threads = 4, parallelize_output_from_storages = 1, input_format_parquet_preserve_order = 0)
WHERE trimLeft(explain) LIKE 'File %';

-- Not split if the order of rows has to be preserved, or if the output must not be parallelized.
SELECT trimLeft(explain) FROM (
    EXPLAIN PIPELINE SELECT sum(id) FROM file(currentDatabase() || '_05394.parquet')
    SETTINGS max_threads = 4, parallelize_output_from_storages = 1, input_format_parquet_preserve_order = 1)
WHERE trimLeft(explain) LIKE 'File %';
SELECT trimLeft(explain) FROM (
    EXPLAIN PIPELINE SELECT sum(id) FROM file(currentDatabase() || '_05394.parquet')
    SETTINGS max_threads = 4, parallelize_output_from_storages = 0)
WHERE trimLeft(explain) LIKE 'File %';
-- Not split if seeks are not allowed: every reader would read the whole file.
SELECT trimLeft(explain) FROM (
    EXPLAIN PIPELINE SELECT sum(id) FROM file(currentDatabase() || '_05394.parquet')
    SETTINGS max_threads = 4, parallelize_output_from_storages = 1, input_format_parquet_preserve_order = 0, input_format_allow_seeks = 0)
WHERE trimLeft(explain) LIKE 'File %';

-- Not more sources than row groups. A small file is used, because the server may lower `max_threads`
-- according to the available memory.
INSERT INTO FUNCTION file(currentDatabase() || '_05394_small.parquet')
SELECT number AS id FROM numbers(8000)
SETTINGS output_format_parquet_row_group_size = 1000, output_format_parquet_use_custom_encoder = 1;

SELECT trimLeft(explain) = 'File × ' || toString((SELECT num_row_groups FROM file(currentDatabase() || '_05394_small.parquet', ParquetMetadata))) || ' 0 → 1'
FROM (
    EXPLAIN PIPELINE SELECT sum(id) FROM file(currentDatabase() || '_05394_small.parquet')
    SETTINGS max_threads = 32, parallelize_output_from_storages = 1, input_format_parquet_preserve_order = 0)
WHERE trimLeft(explain) LIKE 'File %';

-- The results are the same as when the file is read by a single source.
SELECT 'split';
SELECT count(), sum(id), sum(length(s)), sum(arr[2]), sum(t.1), max(t.2) FROM file(currentDatabase() || '_05394.parquet')
SETTINGS max_threads = 8;
SELECT count(), sum(id) FROM file(currentDatabase() || '_05394.parquet') WHERE s = '123'
SETTINGS max_threads = 8;
SELECT count(), sum(id) FROM file(currentDatabase() || '_05394.parquet') PREWHERE id % 7 = 0 WHERE s LIKE '1%'
SETTINGS max_threads = 8;
SELECT count(), sum(id) FROM file(currentDatabase() || '_05394.parquet') WHERE id BETWEEN 25500 AND 74321
SETTINGS max_threads = 8;
SELECT sum(_row_number = id), sum(_row_number) FROM file(currentDatabase() || '_05394.parquet')
SETTINGS max_threads = 8;
SELECT min(id), max(id) FROM file(currentDatabase() || '_05394.parquet')
SETTINGS max_threads = 8;
SELECT id, s FROM file(currentDatabase() || '_05394.parquet') ORDER BY id DESC LIMIT 3
SETTINGS max_threads = 8;
SELECT count() FROM file(currentDatabase() || '_05394.parquet') WHERE _file LIKE '%05394.parquet'
SETTINGS max_threads = 8;

SELECT 'single';
SELECT count(), sum(id), sum(length(s)), sum(arr[2]), sum(t.1), max(t.2) FROM file(currentDatabase() || '_05394.parquet')
SETTINGS max_threads = 1;
SELECT count(), sum(id) FROM file(currentDatabase() || '_05394.parquet') WHERE s = '123'
SETTINGS max_threads = 1;
SELECT count(), sum(id) FROM file(currentDatabase() || '_05394.parquet') PREWHERE id % 7 = 0 WHERE s LIKE '1%'
SETTINGS max_threads = 1;
SELECT count(), sum(id) FROM file(currentDatabase() || '_05394.parquet') WHERE id BETWEEN 25500 AND 74321
SETTINGS max_threads = 1;
SELECT sum(_row_number = id), sum(_row_number) FROM file(currentDatabase() || '_05394.parquet')
SETTINGS max_threads = 1;
SELECT min(id), max(id) FROM file(currentDatabase() || '_05394.parquet')
SETTINGS max_threads = 1;
SELECT id, s FROM file(currentDatabase() || '_05394.parquet') ORDER BY id DESC LIMIT 3
SETTINGS max_threads = 1;
SELECT count() FROM file(currentDatabase() || '_05394.parquet') WHERE _file LIKE '%05394.parquet'
SETTINGS max_threads = 1;
