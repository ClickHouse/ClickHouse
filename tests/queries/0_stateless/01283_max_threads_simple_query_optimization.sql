DROP TABLE IF EXISTS data_01283;

set remote_filesystem_read_method = 'read';
set local_filesystem_read_method = 'pread';

CREATE TABLE data_01283 engine=MergeTree()
ORDER BY key
PARTITION BY key
AS SELECT number key FROM numbers(10);

SET log_queries=1;
SELECT * FROM data_01283 LIMIT 1 FORMAT Null;
SET log_queries=0;
SYSTEM FLUSH LOGS;

-- 1 for PullingAsyncPipelineExecutor::pull
SELECT
    throwIf(count() != 1, 'no query was logged'),
    throwIf(length(thread_ids) != 2, 'too many threads used')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE '%data_01283 LIMIT 1%'
GROUP BY thread_ids
FORMAT Null;

DROP TABLE data_01283;
