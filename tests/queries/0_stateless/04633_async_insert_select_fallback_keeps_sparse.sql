-- Tags: no-parallel-replicas
-- no-parallel-replicas: they reshape the SELECT's blocks, and the byte comparison needs identical blocks on both routes.

-- A multi-block INSERT ... SELECT under async_insert falls back to a synchronous insert, which must receive the
-- same sparse blocks as a plain synchronous INSERT ... SELECT.

DROP TABLE IF EXISTS t_04633_sparse_src;
DROP TABLE IF EXISTS t_04633_sparse_sync;
DROP TABLE IF EXISTS t_04633_sparse_async;

-- A granule of 20000 rows or more would be read as one block.
CREATE TABLE t_04633_sparse_src (id UInt64, s1 String, s2 String, s3 String) ENGINE = MergeTree ORDER BY id
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, index_granularity = 1000;
INSERT INTO t_04633_sparse_src
SELECT number, if(number % 100 = 0, 'a', ''), if(number % 100 = 1, 'b', ''), if(number % 100 = 2, 'c', '')
FROM numbers(20000);

CREATE TABLE t_04633_sparse_sync AS t_04633_sparse_src;
CREATE TABLE t_04633_sparse_async AS t_04633_sparse_src;

-- Several blocks, read in one stream; the trivial INSERT ... SELECT optimization would make it a single block.
SET max_block_size = 5000, max_threads = 1, max_insert_threads = 1, optimize_trivial_insert_select = 0;

INSERT INTO t_04633_sparse_sync SELECT * FROM t_04633_sparse_src SETTINGS async_insert = 0, log_comment = '04633_sparse_sync';
INSERT INTO t_04633_sparse_async SELECT * FROM t_04633_sparse_src
SETTINGS async_insert = 1, wait_for_async_insert = 1, async_insert_select_as_async_insert = 1,
         async_insert_max_data_size = 100000000, log_comment = '04633_sparse_async';

SELECT count() FROM t_04633_sparse_async;

SYSTEM FLUSH LOGS query_log, asynchronous_insert_log, text_log;

-- Fell back: nothing reached the queue.
SELECT count() FROM system.asynchronous_insert_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND database = currentDatabase() AND table = 't_04633_sparse_async';

-- It took the queue route and fell back there.
SELECT count() FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND logger_name = 'AsyncInsertQueueTransform'
    AND message LIKE '%reason: the result is not a single block%'
    AND query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND current_database = currentDatabase() AND type = 'QueryFinish'
            AND log_comment = '04633_sparse_async');

-- The fallback received the same sparse blocks as the synchronous route.
SELECT countIf(log_comment = '04633_sparse_sync'), countIf(log_comment = '04633_sparse_async'),
    anyIf(written_bytes, log_comment = '04633_sparse_async') = anyIf(written_bytes, log_comment = '04633_sparse_sync')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('04633_sparse_sync', '04633_sparse_async');

DROP TABLE t_04633_sparse_src;
DROP TABLE t_04633_sparse_sync;
DROP TABLE t_04633_sparse_async;
