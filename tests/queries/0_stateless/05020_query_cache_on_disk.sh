#!/usr/bin/env bash
# Tests the query cache on disk (setting `query_cache_on_disk_cache_name`) against a server with a preconfigured
# filesystem cache 'cache_for_query_results' (see tests/config/config.d/query_result_cache_on_disk.xml).
# The in-memory query cache is disabled for the test queries, so only the on-disk cache is exercised.
# The cache key incorporates the current database (unique per test run), so entries of previous or parallel runs
# cannot interfere.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

settings="use_query_cache = 1, query_cache_on_disk_cache_name = 'cache_for_query_results', enable_writes_to_query_cache = 0, enable_reads_from_query_cache = 0"

rnd=$(tr -dc 1-9 </dev/urandom | head -c 5) # disambiguates the queries in system.query_log below

user_a="user_05020_a_${CLICKHOUSE_DATABASE}"
user_b="user_05020_b_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_a}, ${user_b}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_a}, ${user_b}"
${CLICKHOUSE_CLIENT} --query "GRANT CURRENT GRANTS ON *.* TO ${user_a}, ${user_b}"

query_private="SELECT 1 == ${rnd} SETTINGS ${settings}"
query_shared="SELECT 2 == ${rnd} SETTINGS ${settings}, query_cache_share_between_users = 1"

echo "-- Basic: the first run computes the result and writes it to disk, the second run reads it from disk"
${CLICKHOUSE_CLIENT} --user "${user_a}" --query "${query_private}"
${CLICKHOUSE_CLIENT} --user "${user_a}" --query "${query_private}"

echo "-- User isolation: another user cannot read the (non-shared) entry and computes the result itself"
${CLICKHOUSE_CLIENT} --user "${user_b}" --query "${query_private}"

echo "-- Sharing: an entry written with query_cache_share_between_users = 1 is readable by another user"
${CLICKHOUSE_CLIENT} --user "${user_a}" --query "${query_shared}"
${CLICKHOUSE_CLIENT} --user "${user_b}" --query "${query_shared}"

echo "-- On-disk cache hits and misses of the runs above (from system.query_log)"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT if(user = '${user_a}', 'user_a', 'user_b'), ProfileEvents['QueryCacheOnDiskHits'], ProfileEvents['QueryCacheOnDiskMisses']
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
        AND current_database = currentDatabase() AND query LIKE 'SELECT 1 == ${rnd} SETTINGS%'
    ORDER BY event_time_microseconds"
${CLICKHOUSE_CLIENT} --query "
    SELECT if(user = '${user_a}', 'user_a', 'user_b'), ProfileEvents['QueryCacheOnDiskHits'], ProfileEvents['QueryCacheOnDiskMisses']
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
        AND current_database = currentDatabase() AND query LIKE 'SELECT 2 == ${rnd} SETTINGS%'
    ORDER BY event_time_microseconds"

echo "-- An unknown filesystem cache name is an error"
${CLICKHOUSE_CLIENT} --query "SELECT 1 SETTINGS use_query_cache = 1, query_cache_on_disk_cache_name = 'no_such_cache_05020'" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

echo "-- An invalid codec is an error"
${CLICKHOUSE_CLIENT} --query "SELECT 1 SETTINGS use_query_cache = 1, query_cache_on_disk_cache_name = 'cache_for_query_results', query_cache_on_disk_codec = 'NO_SUCH_CODEC'" 2>&1 | grep -o -m1 "UNKNOWN_CODEC"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user_a}, ${user_b}"
