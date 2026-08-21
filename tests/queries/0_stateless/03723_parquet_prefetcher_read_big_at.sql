-- Tags: no-fasttest, no-random-settings
-- Closes #90890
-- This tests verifies that while reading a parquet file, the prefetcher leverages more efficient readBigAt logic (ReadMode::RandomRead)

DROP TABLE IF EXISTS t_parquet_prefetcher_read_big_at;

CREATE TABLE t_parquet_prefetcher_read_big_at (a Int32, c String)
ENGINE = S3(s3_conn, filename='test_03723_parquet_prefetcher_read_big_at', format='Parquet');
-- Create parquet file
INSERT INTO t_parquet_prefetcher_read_big_at
    SELECT number, toString(number)
    FROM system.numbers
    LIMIT 1
SETTINGS s3_truncate_on_insert=1;
-- Trigger reading from it
SELECT *
FROM t_parquet_prefetcher_read_big_at
ORDER BY a,c
FORMAT Null
SETTINGS log_comment = 'test_03723_parquet_prefetcher_read_big_at';
-- Ensure that profiling is available for analysis
SYSTEM FLUSH LOGS query_log;
-- Check profiling data to visualize what logic has been used
SELECT
    ProfileEvents['ParquetPrefetcherReadRandomRead'],
    ProfileEvents['ParquetPrefetcherReadSeekAndRead'],
    ProfileEvents['ParquetPrefetcherReadEntireFile']
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query_kind = 'Select' AND current_database = currentDatabase()
    AND log_comment = 'test_03723_parquet_prefetcher_read_big_at'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE IF EXISTS t_parquet_prefetcher_read_big_at;
