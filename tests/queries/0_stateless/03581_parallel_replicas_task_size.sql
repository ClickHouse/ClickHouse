-- Tags: no-fasttest, no-random-settings
-- no-fasttest: requires s3 storage
-- no-random-settings: a lot of settings influence task sizes, so it is simple to disable randomization completely

CREATE TABLE t(a UInt64, s String) ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 's3_cache', min_rows_for_wide_part = 10000, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t;

INSERT INTO t SELECT *, randomString(10) FROM numbers_mt(300_000);
INSERT INTO t SELECT *, randomString(10) FROM numbers(1_000);
INSERT INTO t SELECT *, randomString(10) FROM numbers(1_000);
INSERT INTO t SELECT *, randomString(10) FROM numbers(1_000);
INSERT INTO t SELECT *, randomString(10) FROM numbers(1_000);

-- The problem with too small task sizes specifically happens when we have compact parts.
-- Because for them we don't know individual column sizes, see `calculateMinMarksPerTask()` function.
SELECT
    throwIf(countIf(part_type = 'Compact') = 0),
    throwIf(countIf(part_type = 'Wide') = 0)
FROM system.parts
WHERE (database = currentDatabase()) AND (table = 't')
FORMAT Null;

-- If ClickHouse will choose too small task size, we don't want to artificially correct it's decision.
SET max_threads = 3, merge_tree_min_read_task_size = 1;
-- This test measures the number of requests to the coordinator, which is exactly what the per-task
-- byte floor governs, and it reads from `s3_cache`. `no-random-settings` only covers the per-query
-- randomizer, not a server profile - and the `AutoParallelReplicas` stateless jobs lower this setting
-- in theirs. Pin it to the default so the request count the assertion below checks is the one the
-- default task sizing produces.
SET merge_tree_min_bytes_per_task_for_remote_reading = 2097152;

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 2, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'parallel_replicas';

SELECT * FROM t FORMAT Null SETTINGS log_comment = 'parallel_replicas_task_size_82982938';

SYSTEM FLUSH LOGS query_log;

-- The objective is to check that we request enough marks with each request. Obviously, the more we request, the less requests we will have.
-- Before the fix, in this particular case we made ~ 70 requests, now it should be well under 15.
SELECT throwIf(ProfileEvents['ParallelReplicasNumRequests'] > 15)
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND log_comment = 'parallel_replicas_task_size_82982938' AND type = 'QueryFinish'
SETTINGS enable_parallel_replicas = 0
FORMAT Null;
