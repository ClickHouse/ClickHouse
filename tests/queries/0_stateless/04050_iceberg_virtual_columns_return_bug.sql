-- Tags: no-fasttest
-- Regression test: addRequestedFileLikeStorageVirtualsToChunk used `return`
-- instead of `continue` after adding _row_number, skipping subsequent virtual
-- columns like _data_lake_snapshot_version and causing
-- "Invalid number of columns in chunk pushed to OutputPort".

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '/04050.parquet') SELECT number AS x FROM numbers(5) SETTINGS max_threads = 1;

SELECT _row_number, _data_lake_snapshot_version FROM file(currentDatabase() || '/04050.parquet') ORDER BY _row_number;
