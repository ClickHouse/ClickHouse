-- Tags: use-vectorscan
-- multiMatchAny with non-constant needles adds its regexp cache hits and misses to profile events once per block:
-- one trace_log entry per counter, with the same totals.

SELECT count() FROM numbers(1000) WHERE multiMatchAny(toString(number), ['^' || toString(number % 10)])
    SETTINGS max_block_size = 1000, max_threads = 1, log_comment = '05294_multi_match',
    trace_profile_events = 1, trace_profile_events_list = 'RegexpWithMultipleNeedlesGlobalCacheHit,RegexpWithMultipleNeedlesGlobalCacheMiss';

SYSTEM FLUSH LOGS query_log, trace_log;

SELECT ProfileEvents['RegexpWithMultipleNeedlesGlobalCacheHit'] + ProfileEvents['RegexpWithMultipleNeedlesGlobalCacheMiss']
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment = '05294_multi_match';

SELECT count() <= 2
FROM system.trace_log
WHERE event_date >= yesterday() AND trace_type = 'ProfileEvent'
    AND event IN ('RegexpWithMultipleNeedlesGlobalCacheHit', 'RegexpWithMultipleNeedlesGlobalCacheMiss')
    AND query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
            AND log_comment = '05294_multi_match');
