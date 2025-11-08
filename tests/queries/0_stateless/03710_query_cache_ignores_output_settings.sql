--- Tags: no-parallel
--- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab(c UInt64) ENGINE = Memory AS SELECT 1;

SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, output_format_tsv_crlf_end_of_line = 0;
SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, output_format_tsv_crlf_end_of_line = 1;
SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, http_response_headers = $${'Content-Disposition': 'attachment'}$$;
SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, max_threads = 1;

system flush logs query_log;

select Settings['output_format_tsv_crlf_end_of_line'], Settings['http_response_headers'], Settings['max_threads'], ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses'] from system.query_log where type = 'QueryFinish' and event_time > now() - 600 and current_database = currentDatabase() AND query like 'SELECT \'03710\', %' order by event_time_microseconds limit 1 by query;
