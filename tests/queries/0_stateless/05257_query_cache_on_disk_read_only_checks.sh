#!/usr/bin/env bash
# The checks which protect the query cache against storing wrong results (non-deterministic functions, system tables, non-throw overflow
# modes) apply only if some backend can actually store the result. `clickhouse-local` has no in-memory query cache, so with
# `enable_writes_to_query_cache_on_disk = 0` (or without a query cache on disk) nothing can be written and the query must run normally.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05257_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05257_config_${CLICKHOUSE_DATABASE}.yaml"
rm -rf "${CACHE_DIR}"

cat > "${CONFIG_FILE}" <<EOF_CONFIG
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
EOF_CONFIG

settings="use_query_cache = true, query_cache_on_disk_cache_name = 'query_results'"

echo "-- Query cache on disk in read-only mode: the queries run normally"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    SELECT rand() >= 0 SETTINGS ${settings}, enable_writes_to_query_cache_on_disk = false;
    SELECT count() FROM system.one SETTINGS ${settings}, enable_writes_to_query_cache_on_disk = false;
    SELECT count() FROM numbers(10) SETTINGS ${settings}, enable_writes_to_query_cache_on_disk = false, read_overflow_mode = 'break';"

echo "-- No query cache on disk: the queries run normally"
${CLICKHOUSE_LOCAL} --query "
    SELECT rand() >= 0 SETTINGS use_query_cache = true;
    SELECT count() FROM system.one SETTINGS use_query_cache = true;
    SELECT count() FROM numbers(10) SETTINGS use_query_cache = true, read_overflow_mode = 'break';"

echo "-- Query cache on disk with writes: the checks apply"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SELECT rand() SETTINGS ${settings}" 2>&1 | grep -o "QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SELECT count() FROM system.one SETTINGS ${settings}" 2>&1 | grep -o "QUERY_CACHE_USED_WITH_SYSTEM_TABLE"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SELECT count() FROM numbers(10) SETTINGS ${settings}, read_overflow_mode = 'break'" 2>&1 | grep -o "QUERY_CACHE_USED_WITH_NON_THROW_OVERFLOW_MODE"

echo "-- Nothing was written by the read-only queries"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    SELECT count() FROM numbers(10) SETTINGS ${settings}, enable_writes_to_query_cache_on_disk = false, read_overflow_mode = 'break';
    SELECT event FROM system.events WHERE event LIKE 'QueryCacheOnDisk%' AND value > 0 ORDER BY event;"

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
