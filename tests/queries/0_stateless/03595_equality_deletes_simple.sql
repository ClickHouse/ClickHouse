-- Tags: no-fasttest, no-parallel-replicas

SELECT sum(id) FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');
SELECT sum(id) FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename = 'deletes_db/eq_deletes_table');
SELECT sum(id), count(name) FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');

-- Test equality deletes are reported in system.iceberg_files.
DROP TABLE IF EXISTS eq_deletes_t;
CREATE TABLE eq_deletes_t
ENGINE = IcebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');

SELECT '--- file counts by content type ---';
SELECT content, count()
FROM system.iceberg_files
WHERE database = currentDatabase() AND table = 'eq_deletes_t'
GROUP BY content
ORDER BY content;

SELECT '--- equality delete files ---';
SELECT
    content,
    sequence_number,
    record_count,
    equality_ids,
    upper(file_format)              AS file_format,
    endsWith(file_path, '.parquet') AS file_path_ends_with_parquet,
    file_size_in_bytes > 0          AS file_size_positive
FROM system.iceberg_files
WHERE database = currentDatabase()
  AND table = 'eq_deletes_t'
  AND content = 'EQUALITY_DELETE'
ORDER BY equality_ids, sequence_number
FORMAT Vertical;

DROP TABLE eq_deletes_t;
