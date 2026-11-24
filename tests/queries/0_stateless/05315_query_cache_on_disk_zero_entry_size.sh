#!/usr/bin/env bash
# A maximum entry size of 0 (`query_cache.max_entry_size_in_bytes` or `query_cache.max_entry_size_in_rows`) means that nothing may be
# cached, in both backends of the query cache. So with only these limits at 0, nothing is written to the query cache on disk, and the
# checks which protect against storing wrong results (here: non-deterministic functions) do not apply.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05315_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05315_config_${CLICKHOUSE_DATABASE}.yaml"

settings="use_query_cache = true, query_cache_on_disk_cache_name = 'query_results'"
events_query="SELECT event FROM system.events WHERE event LIKE 'QueryCacheOnDisk%' AND value > 0 ORDER BY event"

for limit in max_entry_size_in_bytes max_entry_size_in_rows
do
    rm -rf "${CACHE_DIR}"
    cat > "${CONFIG_FILE}" <<EOF_CONFIG
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
query_cache:
    ${limit}: 0
EOF_CONFIG

    echo "-- ${limit} = 0: the queries run normally and nothing is written"
    ${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
        SELECT rand() >= 0 SETTINGS ${settings};
        SELECT 1 SETTINGS ${settings};
        SELECT count() FROM numbers(0) SETTINGS ${settings};
        ${events_query};"

    echo "-- ${limit} = 0: nothing is read in a new process"
    ${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
        SELECT 1 SETTINGS ${settings};
        ${events_query};"
done

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
