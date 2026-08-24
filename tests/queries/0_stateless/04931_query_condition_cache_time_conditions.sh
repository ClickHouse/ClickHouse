#!/usr/bin/env bash
# Tags: no-parallel, no-parallel-replicas
# no-parallel: drops the (instance-wide) query condition cache
# no-parallel-replicas: the query condition cache is populated per replica

# Tests deriving deterministic conditions from conditions involving the current time for the query
# condition cache (issue #115504): a condition like `time >= now() - INTERVAL 100 DAY` is cached
# under the hash of a derived condition with the folded current-time constant rounded onto a time
# grid, instead of not being cached at all.
#
# The tests with a grid-aligned constant (`time >= today() - 100`) run in a retry loop: the cache
# key intentionally rotates once per grid cell (once per day here), so a test that straddles
# midnight can lose the cache entry between the priming query and the probing query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# enable_analyzer = 1: the query condition cache only works with the analyzer (query_info has no
# filter DAG without it), like in the other query_condition_cache tests.
settings="use_query_condition_cache = true, use_query_condition_cache_for_time_conditions = true, enable_analyzer = 1"

# A single part that mixes 'old' rows (which no current-time condition matches) with recent rows,
# ordered so that the old rows fill whole granules of their own. A separate all-old part would be
# pruned by part-level statistics before the query condition cache ever sees it; a part that spans
# both ranges can only be pruned granule-wise, which is exactly what the cache provides.
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS tab;
    CREATE TABLE tab (time DateTime, x UInt64) ENGINE = MergeTree ORDER BY x
        SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 8192;
    INSERT INTO tab
    SELECT if(number < 1_000_000, toDateTime('2000-01-01 00:00:00') + (number % 86400), now() - (number % 3600)), number
    FROM numbers(2_000_000)
    SETTINGS max_insert_threads = 1, max_block_size = 2_000_000, min_insert_block_size_rows = 2_000_000, min_insert_block_size_bytes = 0;
"

function scenario()
{
    local name="$1"
    local condition="$2"
    local move_to_prewhere="$3"

    ${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"

    local query="SELECT sum(x) FROM tab WHERE ${condition} SETTINGS ${settings}, optimize_move_to_prewhere = ${move_to_prewhere} FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "${query} -- prime ${name}"
    local entries
    entries=$(${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.query_condition_cache")

    ${CLICKHOUSE_CLIENT} --query "${query} -- probe ${name}"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    local hits
    hits=$(${CLICKHOUSE_CLIENT} --query "
        SELECT ProfileEvents['QueryConditionCacheHits'] > 0
            AND toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND type = 'QueryFinish'
            AND current_database = currentDatabase()
            AND endsWith(query, '-- probe ${name}')
        ORDER BY event_time_microseconds DESC
        LIMIT 1")

    echo "${entries} ${hits}"
}

function scenario_with_retries()
{
    local name="$1"
    # Retried in case the priming and the probing query straddle a midnight (see above).
    for _ in 1 2 3; do
        result=$(scenario "$name" "$2" "$3")
        if [ "${result}" == "1 1" ]; then
            break
        fi
    done
    echo "${name}: ${result}"
}

# A grid-aligned constant: the derived condition is the same for reads and writes, so the very next
# query can already use the cache to skip the granules that hold only old rows.
scenario_with_retries "aligned, PREWHERE" "time >= today() - 100" "true"
scenario_with_retries "aligned, WHERE" "time >= today() - 100" "false"

# A non-aligned constant: entries are written (under the condition rounded up), but they only serve
# queries in the next grid cell (the next day), so no hit is expected here. Only check that the
# condition is cached at all.
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(x) FROM tab WHERE time >= now() - INTERVAL 100 DAY
    SETTINGS ${settings} FORMAT Null"
echo -n "non-aligned, entries cached: "
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.query_condition_cache"

# With the setting disabled, conditions involving the current time are not cached at all.
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(x) FROM tab WHERE time >= today() - 100
    SETTINGS use_query_condition_cache = true, use_query_condition_cache_for_time_conditions = false, enable_analyzer = 1 FORMAT Null"
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(x) FROM tab WHERE time >= now() - INTERVAL 100 DAY
    SETTINGS use_query_condition_cache = true, use_query_condition_cache_for_time_conditions = false, enable_analyzer = 1 FORMAT Null"
echo -n "disabled, entries cached: "
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.query_condition_cache"

${CLICKHOUSE_CLIENT} --query "DROP TABLE tab"
