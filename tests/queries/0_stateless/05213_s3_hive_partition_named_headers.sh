#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: needs Minio

# Of the object storage configurations only `Web` registers `_headers` as a response-header `Map`.
# On `s3`/`azureBlobStorage`/`hdfs` a `_headers` of that name is a Hive path key and must resolve to
# the path value.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_hive_headers"
AUTH="'test', 'testtest'"

$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION s3('${URL}/_headers=abc/data.tsv', ${AUTH}, 'TSV', 'x UInt64')
SELECT 1 SETTINGS s3_truncate_on_insert = 1;
"

$CLICKHOUSE_CLIENT --use_hive_partitioning=1 -q "
SELECT _headers FROM s3('${URL}/_headers=abc/data.tsv', ${AUTH}, 'TSV', 'x UInt64');
"

# A read of only virtual columns takes the number of rows from the cache the second time, as for any
# other Hive key. The cache drops an entry registered in the second the object was modified.
sleep 1
for run in 1 2; do
    $CLICKHOUSE_CLIENT --use_hive_partitioning=1 --log_comment="05213_count_${run}" -q "
    SELECT _headers, count() FROM s3('${URL}/_headers=abc/data.tsv', ${AUTH}, 'TSV', 'x UInt64') GROUP BY _headers
    SETTINGS use_cache_for_count_from_files = 1, optimize_count_from_files = 1;
    "
done

$CLICKHOUSE_CLIENT -q "
SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['SchemaInferenceCacheNumRowsHits']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND initial_query_id IN (
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
        AND log_comment = '05213_count_2');
"
