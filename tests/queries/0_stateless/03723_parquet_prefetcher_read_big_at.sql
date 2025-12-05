DROP TABLE IF EXISTS t_parquet_prefetcher_read_big_at;

CREATE TABLE t_parquet_prefetcher_read_big_at (a Int32, c String)
ENGINE = S3(s3_conn, format='Parquet');

INSERT INTO t_parquet_prefetcher_read_big_at
    SELECT number, toString(number)
    FROM system.numbers
    LIMIT 2
SETTINGS s3_truncate_on_insert=1;

SELECT * FROM t_parquet_prefetcher_read_big_at;

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['ParquetPrefetcherReadRandomRead'],
    ProfileEvents['ParquetPrefetcherReadSeekAndRead'],
    ProfileEvents['ParquetPrefetcherReadEntireFile']
FROM system.query_log
WHERE query == 'SELECT * FROM t_parquet_prefetcher_read_big_at;'
    AND current_database = currentDatabase()
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE IF EXISTS t_parquet_prefetcher_read_big_at;
