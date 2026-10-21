-- A hash join built by several workers frees a right table of many heap objects on the `HashJoinDtor` pool.
-- The thread that destroys the join frees a table built by a single worker, and a table of few objects,
-- however many bytes they hold. `query_log.thread_ids` lists the pool threads too, but they come from the
-- global pool and also run the pipeline, so only the thread names in `system.query_thread_log` show the pool.

SET join_algorithm = 'hash';
SET max_threads = 4;
SET parallel_hash_join_threshold = 0;
-- Otherwise the small side becomes the build side.
SET query_plan_join_swap_table = 'false';
-- The serial case needs the real row estimate, which CI may skip with a limit of 0 or randomize.
SET query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
-- The build keeps blocks of `max_block_size` rows instead of squashing them.
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;
SET log_queries = 1, log_query_threads = 1, log_queries_min_query_duration_ms = 0;

-- 1000 blocks of four columns. The thread fuzzer of CI slows down every block, so wide blocks keep the build fast.
SELECT count(), uniqExact(r.a, r.b, r.c, r.d)
FROM numbers(10) AS l
JOIN (SELECT number AS k, toString(number) AS a, toString(number + 1) AS b, toString(number + 2) AS c, toString(number + 3) AS d
    FROM numbers(10000)) AS r ON l.number = r.k
SETTINGS log_comment = 'many_objects', max_block_size = 10;

-- 120 blocks, each with one column of about 1 MB.
SELECT count(), uniqExact(r.s)
FROM numbers(10) AS l
JOIN (SELECT number AS k, repeat('x', 1000) AS s FROM numbers(120000)) AS r ON l.number = r.k
SETTINGS log_comment = 'few_objects', max_block_size = 1000;

-- The build stops at the row limit and never reaches `onBuildPhaseFinish`, so the lists stay per worker.
SELECT count(), uniqExact(r.a, r.b, r.c, r.d)
FROM numbers(10) AS l
JOIN (SELECT number AS k, toString(number) AS a, toString(number + 1) AS b, toString(number + 2) AS c, toString(number + 3) AS d
    FROM numbers(10000)) AS r ON l.number = r.k
SETTINGS log_comment = 'cancelled', max_block_size = 10, max_rows_in_join = 9500; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- The row count of the `Memory` table is below the threshold, so one worker builds its 1250 blocks of four columns.
-- Debug and sanitizer builds walk all blocks of one worker after each insert, so few wide blocks keep the build fast.
CREATE TABLE right_memory (k UInt64, a String, b String, c String, d String) ENGINE = Memory;
INSERT INTO right_memory
SELECT number, toString(number), toString(number + 1), toString(number + 2), toString(number + 3) FROM numbers(12500)
SETTINGS max_block_size = 10, max_insert_block_size = 10, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SELECT count(), uniqExact(r.a, r.b, r.c, r.d)
FROM numbers(10) AS l
JOIN right_memory AS r ON l.number = r.k
SETTINGS log_comment = 'serial', parallel_hash_join_threshold = 1000000000;
DROP TABLE right_memory;

SYSTEM FLUSH LOGS query_log, query_thread_log;

SELECT
    log_comment,
    query_id IN (
        SELECT query_id
        FROM system.query_thread_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND thread_name = 'HashJoinDtor'
    ) AS freed_on_pool
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type != 'QueryStart'
    AND log_comment IN ('many_objects', 'few_objects', 'cancelled', 'serial')
ORDER BY event_time_microseconds;
