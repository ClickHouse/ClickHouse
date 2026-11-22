-- Regexp cache hits and misses of LIKE and match with a non-constant pattern are counted exactly.

SELECT count() FROM numbers(1000) WHERE like(toString(number), '%' || toString(number % 10) || '_')
    SETTINGS max_block_size = 1000, max_threads = 1, log_comment = '05292_string';
SELECT count() FROM numbers(1000) WHERE like(toFixedString(toString(number), 3), '%' || toString(number % 10) || '_')
    SETTINGS max_block_size = 1000, max_threads = 1, log_comment = '05292_fixed_string';
SELECT count() FROM numbers(1000) WHERE match('12345', '.*' || toString(number % 10) || '.')
    SETTINGS max_block_size = 1000, max_threads = 1, log_comment = '05292_const_haystack';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['RegexpLocalCacheHit'], ProfileEvents['RegexpLocalCacheMiss']
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND startsWith(log_comment, '05292_')
ORDER BY log_comment;
