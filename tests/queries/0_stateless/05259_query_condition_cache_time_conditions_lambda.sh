#!/usr/bin/env bash
# Tags: no-parallel, no-parallel-replicas
# no-parallel: drops the (instance-wide) query condition cache
# no-parallel-replicas: the query condition cache is populated per replica

# A condition involving the current time is cached under a derived condition only if the rest of
# the condition is deterministic. A non-deterministic function hidden in the body of a lambda
# (`arrayExists(a -> rand() % 2 = 0, arr)`) makes the whole condition non-deterministic, including
# when the lambda is constant-folded into a `ColumnFunction` carrier.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# enable_analyzer = 1: the query condition cache only works with the analyzer.
common_settings="use_query_condition_cache = true, use_query_condition_cache_for_time_conditions = true, enable_analyzer = 1"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS tab;
    CREATE TABLE tab (time DateTime, x UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY x
        SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 8192;
    INSERT INTO tab
    SELECT if(number < 100_000, toDateTime('2000-01-01 00:00:00') + (number % 86400), now() - (number % 3600)), number, [number, number + 1]
    FROM numbers(200_000)
    SETTINGS max_insert_threads = 1, max_block_size = 200_000, min_insert_block_size_rows = 200_000, min_insert_block_size_bytes = 0;
"

function entries_after()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM tab WHERE $1 SETTINGS ${common_settings} FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.query_condition_cache"
}

echo -n "deterministic lambda: "
entries_after "time >= now() - INTERVAL 10 DAY AND arrayExists(a -> a % 2 = 0, arr)"
echo -n "non-deterministic lambda: "
entries_after "time >= now() - INTERVAL 10 DAY AND arrayExists(a -> rand() % 2 = 0, arr)"
echo -n "non-deterministic nested lambda: "
entries_after "time >= now() - INTERVAL 10 DAY AND arrayExists(a -> arrayExists(b -> rand() % 2 = 0, [a]), arr)"

# A lambda capturing the current time is constant-folded into a `ColumnFunction` carrier; the time
# constant inside it is not rounded, so the condition must not be cached either.
echo -n "current time inside lambda: "
entries_after "time >= now() - INTERVAL 10 DAY AND arrayExists(a -> a < toUInt64(toUnixTimestamp(now())), arr)"

${CLICKHOUSE_CLIENT} --query "DROP TABLE tab"
