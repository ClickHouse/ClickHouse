#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: the query from query_log errors due to missing columns.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query-cache-tag "${CLICKHOUSE_DATABASE}" <<END

SET max_block_size = 100;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab(c UInt64) ENGINE = Memory AS SELECT 1;

SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, output_format_tsv_crlf_end_of_line = 0;
SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, output_format_tsv_crlf_end_of_line = 1;
SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, http_response_headers = \$\${'Content-Disposition': 'attachment'}\$\$;
SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, max_block_size = 1;
SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, max_block_size = 1;
SELECT '03710', c FROM tab FORMAT CSV SETTINGS use_query_cache = 1, max_block_size = 1; -- here it is miss due to a different AST structure, but better if it will be a hit
SELECT '03710', c FROM tab FORMAT TSV SETTINGS use_query_cache = 1, max_block_size = 1;
SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, max_block_size = 1 FORMAT CSV;
SELECT '03710', c FROM tab SETTINGS use_query_cache = 1, max_block_size = 1 FORMAT TSV;

system flush logs query_log;

select Settings['output_format_tsv_crlf_end_of_line'], Settings['http_response_headers'], Settings['max_block_size'], ProfileEvents['QueryCacheHits'] > 0 ? 'hit' : '', ProfileEvents['QueryCacheMisses'] > 0 ? 'miss' : '' from system.query_log where type = 'QueryFinish' and event_time > now() - 600 and current_database = currentDatabase() AND query like 'SELECT \'03710\', %' order by event_time_microseconds;
END
