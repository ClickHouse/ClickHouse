#!/usr/bin/env bash
# Concurrent queries with the same key of the query cache on disk (setting `query_cache_on_disk_cache_name`): only one of the
# concurrent writers stores the entry, the others skip the insert, a reader never sees a half-written entry, every query returns
# the correct result, and once the entry is there, everybody is served from disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The in-memory query cache is disabled, so that every query goes to the disk. The tag makes the key unique to this test run.
tag="05235_${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}"
settings="use_query_cache = 1, query_cache_on_disk_cache_name = 'cache_for_query_results', enable_reads_from_query_cache = 0, enable_writes_to_query_cache = 0, query_cache_tag = '${tag}'"
query="SELECT sum(cityHash64(number)), count() FROM numbers(300000) SETTINGS ${settings}"

expected=$(${CLICKHOUSE_CLIENT} --query "SELECT sum(cityHash64(number)), count() FROM numbers(300000)")

rounds=4
clients=16
for _ in $(seq 1 ${rounds}); do
    for _ in $(seq 1 ${clients}); do
        ${CLICKHOUSE_CLIENT} --query "${query}" &
    done
    wait
done | sort | uniq -c | sed "s/${expected}/<expected result>/"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log, text_log"

${CLICKHOUSE_CLIENT} --query "
    WITH (SELECT groupArray(query_id) FROM system.query_log
          WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE '%query_cache_tag = ''${tag}''%') AS ids
    SELECT
        'queries', count(),
        'hits + misses', sum(ProfileEvents['QueryCacheOnDiskHits']) + sum(ProfileEvents['QueryCacheOnDiskMisses']),
        'all rounds but the first were served from disk', sum(ProfileEvents['QueryCacheOnDiskHits']) >= $((rounds * clients - clients)),
        'the entry was written once', sum(ProfileEvents['QueryCacheOnDiskWrittenBytes'] > 0),
        'no errors or warnings logged', (SELECT count() FROM system.text_log
                                           WHERE logger_name = 'QueryResultCacheOnDisk' AND level IN ('Fatal', 'Critical', 'Error', 'Warning') AND has(ids, query_id))
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND has(ids, query_id)
    FORMAT TSV"
