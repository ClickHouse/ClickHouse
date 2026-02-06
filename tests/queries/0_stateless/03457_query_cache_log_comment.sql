--- Tags: no-parallel
--- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab(c UInt64) ENGINE = Memory AS SELECT 1;

SELECT c FROM tab SETTINGS use_query_cache = 1, log_comment='';
SELECT c FROM tab SETTINGS use_query_cache = 1, log_comment='aaa';
SELECT c FROM tab SETTINGS use_query_cache = 1, log_comment='bbb';

system flush logs query_log;

select log_comment, ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses'] from system.query_log where type = 'QueryFinish' and event_time > now() - 600 and current_database = currentDatabase() AND query like 'SELECT c FROM tab SETTINGS use_query_cache%' order by event_time_microseconds;
