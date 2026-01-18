-- Tags: no-parallel-replicas
-- no-parallel-replicas: the query from query_log errors due to missing columns.

-- Checks that the query cache ignores output format related settings (settings starting with 'output_format_')

SET max_block_size = 100;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab(c UInt64) ENGINE = Memory AS SELECT 1;

SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, output_format_tsv_crlf_end_of_line = 0;
SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, output_format_tsv_crlf_end_of_line = 1;
SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, max_block_size = 1;
SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, max_block_size = 1;
SELECT '03710', c FROM tab FORMAT CSV SETTINGS use_query_cache = 1, max_block_size = 1; -- Same query as before but with different FORMAT, unfortunately that's a miss because the query cache uses the AST structure as key
SELECT '03710', c FROM tab FORMAT TSV SETTINGS use_query_cache = 1, max_block_size = 1;
SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, max_block_size = 1 FORMAT CSV;
SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, max_block_size = 1 FORMAT TSV;

SYSTEM FLUSH LOGS query_log;

SELECT
    Settings['output_format_tsv_crlf_end_of_line'],
    Settings['max_block_size'],
    ProfileEvents['QueryCacheHits'] > 0 ? 'hit' : '',
    ProfileEvents['QueryCacheMisses'] > 0 ? 'miss' : ''
FROM
    system.query_log
WHERE
    type = 'QueryFinish'
    AND event_time > now() - 600
    AND current_database = currentDatabase()
    AND query LIKE 'SELECT \'03710\', %'
ORDER BY
    event_time_microseconds;
