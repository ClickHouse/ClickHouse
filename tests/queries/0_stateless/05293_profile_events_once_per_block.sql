-- Profile events are not updated once per row by CAST to Nullable(Bool) (IOBufferAllocs) and arrayIntersect over
-- non-numeric elements (ArenaAllocChunks); the arrayIntersect memory stays bounded over a block with large elements.

SELECT count() FROM numbers(1000) WHERE CAST(toString(number % 2) AS Nullable(Bool))
    SETTINGS max_block_size = 1000, max_threads = 1, log_comment = '05293_nullable_bool';
SELECT count() FROM numbers(1000) WHERE notEmpty(arrayIntersect([toDateTime64(number % 10, 0)], [toDateTime64(1, 0), toDateTime64(2, 0)]))
    SETTINGS max_block_size = 1000, max_threads = 1, log_comment = '05293_array_intersect';
SELECT count() FROM numbers(5000) WHERE notEmpty(arrayIntersect([tuple(repeat('x', 10000))], [tuple(toString(number)), tuple('')]))
    SETTINGS max_block_size = 5000, max_threads = 1, max_memory_usage = 20000000;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['IOBufferAllocs'] < 100,
    ProfileEvents['ArenaAllocChunks'] < 100
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment IN ('05293_array_intersect', '05293_nullable_bool')
ORDER BY log_comment;
