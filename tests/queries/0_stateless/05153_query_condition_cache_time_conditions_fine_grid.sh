#!/usr/bin/env bash
# Tags: no-parallel, no-parallel-replicas
# no-parallel: drops the (instance-wide) query condition cache
# no-parallel-replicas: the query condition cache is populated per replica

# A small positive `query_condition_cache_time_condition_grid_factor` must make the derived
# condition converge to the original one, not disable the derivation: the requested grid step is
# clamped to one second. In particular a constant that is already aligned to a coarse grid
# (`today() - 100`) survives the rounding unchanged in both directions, so the cache stays usable
# no matter how small the factor is.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# enable_analyzer = 1: the query condition cache only works with the analyzer (query_info has no
# filter DAG without it), like in the other query_condition_cache tests.
common_settings="use_query_condition_cache = true, use_query_condition_cache_for_time_conditions = true, enable_analyzer = 1"

# A single part mixing 'old' rows (which the condition does not match) with recent rows, so that the
# old rows fill whole granules of their own and can only be pruned granule-wise - which is what the
# query condition cache provides.
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
    local factor="$3"

    ${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"

    local query="SELECT sum(x) FROM tab WHERE ${condition}
        SETTINGS ${common_settings}, query_condition_cache_time_condition_grid_factor = ${factor} FORMAT Null"
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
    # The cache key rotates once per grid cell, so a run that straddles the end of a cell can lose
    # the entry between the priming and the probing query.
    for _ in 1 2 3; do
        result=$(scenario "$name" "$2" "$3")
        if [ "${result}" == "1 1" ]; then
            break
        fi
    done
    echo "${name}: ${result}"
}

# The constant is close to the current time, so a small factor asks for a grid step far below one
# second. The step is clamped to one second, which is the identity for these whole-second constants,
# so the aligned constant derives the same condition for the write and the read side and the cache
# is usable right away - instead of the feature switching itself off.
scenario_with_retries "hour-aligned, small factor" "time >= toStartOfHour(now())" "0.000001"
scenario_with_retries "day-aligned, small factor" "time >= today()" "0.000001"
# The same for a distant constant, where an even smaller factor is needed to get below one second.
scenario_with_retries "distant, denormal factor" "time >= today() - 100" "1e-30"
# For comparison: the default factor.
scenario_with_retries "distant, default factor" "time >= today() - 100" "1"

# A non-positive factor disables the derivation entirely.
for factor in 0 -1; do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT sum(x) FROM tab WHERE time >= toStartOfHour(now())
        SETTINGS ${common_settings}, query_condition_cache_time_condition_grid_factor = ${factor} FORMAT Null"
    echo -n "factor ${factor}, entries cached: "
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.query_condition_cache"
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE tab"
