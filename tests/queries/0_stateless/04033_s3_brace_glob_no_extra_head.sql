-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: Depends on AWS
-- Tag no-parallel-replicas: test counts exact S3 API calls per query, which differs under parallel replicas

-- Verify that reading from S3 with brace-expansion globs like {a,b,c}.tsv
-- does not produce extra HeadObject requests for hive partitioning sample path resolution.
-- With the fix, getPathSample() expands the glob locally instead of creating a file iterator.

SET max_threads = 1;
SET use_hive_partitioning=1;

SELECT * FROM s3('http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64')
ORDER BY c1, c2, c3;

SYSTEM FLUSH LOGS query_log;

-- With use_hive_partitioning=1 (default), getPathSample() used to create a redundant file iterator,
-- causing N+1 HeadObject calls (4 instead of 3 for 3 files).
-- After the fix, brace-expansion globs are resolved locally, so we expect exactly 3 HeadObject calls
-- (one per file from the read iterator) and 0 ListObjects calls (enum globs skip listing).
SELECT
    ProfileEvents['S3HeadObject'],
    ProfileEvents['S3ListObjects']
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query ILIKE 'SELECT%FROM s3%test/%{a,b,c}.tsv%ORDER BY%';
