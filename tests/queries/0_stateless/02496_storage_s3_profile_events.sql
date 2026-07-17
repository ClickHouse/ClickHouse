-- Tags: no-parallel, no-fasttest, no-random-settings

DROP TABLE IF EXISTS t_s3_events_02496;

CREATE TABLE t_s3_events_02496 (a UInt64)
ENGINE = S3(s3_conn, filename = 'test_02496_{_partition_id}', format = Parquet)
PARTITION BY a;

INSERT INTO t_s3_events_02496 SELECT number FROM numbers(10) SETTINGS s3_truncate_on_insert=1;

SET max_threads = 1;
SET parallel_replicas_for_cluster_engines = 0;
SELECT count() FROM s3(s3_conn, filename = 'test_02496_*', format = Parquet, structure = 'a UInt64');
SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['S3HeadObject'],
    ProfileEvents['S3ListObjects'],
    ProfileEvents['RemoteFSPrefetches'],
    ProfileEvents['IOBufferAllocBytes'] < 100000
FROM system.query_log WHERE current_database = currentDatabase()
AND type = 'QueryFinish' AND query ILIKE 'SELECT count() FROM s3%test_02496%';

DROP TABLE t_s3_events_02496;
