-- Tags: no-fasttest
-- no-fasttest because of Parquet

-- A `_path` / `_file` filter whose set is built only when the pipeline runs (`IN (subquery)`)
-- is deferred to `StorageFileSource::FilesIterator::next`. The per-bucket sources of the
-- single-file split never call it, so the split must be disabled for such a query: the file
-- has to be pruned before it is opened.

INSERT INTO FUNCTION file(currentDatabase() || '_05331.parquet') SELECT * FROM numbers(3200)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 50;

SET max_threads = 8, parallelize_output_from_storages = 1,
    input_format_parquet_min_bytes_to_split = 0, input_format_parquet_bytes_per_split_bucket = 0;

-- Without a deferred filter the file is split into several sources.
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT sum(number) FROM file(currentDatabase() || '_05331.parquet'))
    WHERE explain LIKE '%File × %';

-- With a deferred filter it is read through a single source, which applies the filter.
SELECT count() FROM (EXPLAIN PIPELINE SELECT sum(number) FROM file(currentDatabase() || '_05331.parquet') WHERE _file IN (SELECT 'none'))
    WHERE explain LIKE '%File × %';

SELECT sum(number) FROM file(currentDatabase() || '_05331.parquet') WHERE _file IN (SELECT currentDatabase() || '_05331.parquet');
SELECT sum(number) FROM file(currentDatabase() || '_05331.parquet') WHERE _file IN (SELECT 'none');
SELECT sum(number) FROM file(currentDatabase() || '_05331.parquet') WHERE _path NOT IN (SELECT _path FROM file(currentDatabase() || '_05331.parquet', 'One'));
