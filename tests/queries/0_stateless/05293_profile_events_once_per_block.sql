-- Tags: use-vectorscan
-- Profile events are updated once per block, not once per row, by CAST to Nullable(Bool) (IOBufferAllocs),
-- arrayIntersect over non-numeric elements (ArenaAllocChunks) and multiMatchAny with non-constant needles.

SELECT count() FROM numbers(1000) WHERE CAST(toString(number % 2) AS Nullable(Bool))
    SETTINGS max_block_size = 1000, max_threads = 1, log_comment = '05293_nullable_bool';
SELECT count() FROM numbers(1000) WHERE notEmpty(arrayIntersect([toDateTime64(number % 10, 0)], [toDateTime64(1, 0), toDateTime64(2, 0)]))
    SETTINGS max_block_size = 1000, max_threads = 1, log_comment = '05293_array_intersect';
SELECT count() FROM numbers(1000) WHERE multiMatchAny(toString(number), ['^' || toString(number % 10)])
    SETTINGS max_block_size = 1000, max_threads = 1, log_comment = '05293_multi_match';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['IOBufferAllocs'] < 100,
    ProfileEvents['ArenaAllocChunks'] < 100,
    ProfileEvents['RegexpWithMultipleNeedlesGlobalCacheHit'] + ProfileEvents['RegexpWithMultipleNeedlesGlobalCacheMiss']
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND startsWith(log_comment, '05293_')
ORDER BY log_comment;
