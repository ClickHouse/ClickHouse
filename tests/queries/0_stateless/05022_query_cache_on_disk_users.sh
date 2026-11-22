#!/usr/bin/env bash
# A non-shared entry of the query cache on disk belongs to the user who wrote it: it must neither be served to nor block the entry of
# another user. A shared entry (setting `query_cache_share_between_users`) is served to every user.
# Only the on-disk backend is exercised, reads from and writes to the in-memory query cache are disabled.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_a="user_a_${CLICKHOUSE_DATABASE}"
user_b="user_b_${CLICKHOUSE_DATABASE}"
table="${CLICKHOUSE_DATABASE}.tab"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${table} (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO ${table} SELECT number FROM numbers(100);
    DROP USER IF EXISTS ${user_a}, ${user_b};
    CREATE USER ${user_a}, ${user_b};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user_a}, ${user_b};
"

# `cache_for_query_results` is the filesystem cache preconfigured for tests, see tests/config/config.d/query_result_cache_on_disk.xml
settings="use_query_cache = true, query_cache_on_disk_cache_name = 'cache_for_query_results', enable_reads_from_query_cache = false, enable_writes_to_query_cache = false"

run() # user, query_id, query, extra settings
{
    # ${CLICKHOUSE_CLIENT} already selects the test database, so both users run with it as their current database: it is part of the
    # cache key, and it is what restricts the `system.query_log` lookups below.
    ${CLICKHOUSE_CLIENT} --user "$1" --query_id "$2" --query "$3 SETTINGS ${settings}${4:+, $4}" > /dev/null
}

hits() # query_id
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT ProfileEvents['QueryCacheOnDiskHits'] FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$1' AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC LIMIT 1
    "
}

echo "-- A non-shared entry written by user A is not served to user B, and user B stores its own entry"
private_query="SELECT count() FROM ${table} WHERE a != 42"
run "${user_a}" "05022_a_private_${CLICKHOUSE_DATABASE}" "${private_query}"
run "${user_b}" "05022_b_private_1_${CLICKHOUSE_DATABASE}" "${private_query}"
run "${user_b}" "05022_b_private_2_${CLICKHOUSE_DATABASE}" "${private_query}"

echo "-- A shared entry written by user A is served to user B"
shared_query="SELECT count() FROM ${table} WHERE a != 43"
run "${user_a}" "05022_a_shared_${CLICKHOUSE_DATABASE}" "${shared_query}" "query_cache_share_between_users = true"
run "${user_b}" "05022_b_shared_${CLICKHOUSE_DATABASE}" "${shared_query}"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

echo -n "user A, first run of the non-shared query, hits: "; hits "05022_a_private_${CLICKHOUSE_DATABASE}"
echo -n "user B, first run of the non-shared query, hits: "; hits "05022_b_private_1_${CLICKHOUSE_DATABASE}"
echo -n "user B, second run of the non-shared query, hits: "; hits "05022_b_private_2_${CLICKHOUSE_DATABASE}"
echo -n "user A, first run of the shared query, hits: "; hits "05022_a_shared_${CLICKHOUSE_DATABASE}"
echo -n "user B, first run of the shared query, hits: "; hits "05022_b_shared_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user_a}, ${user_b}"
