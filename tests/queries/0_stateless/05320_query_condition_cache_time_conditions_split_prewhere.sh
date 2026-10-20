#!/usr/bin/env bash
# Tags: no-parallel, no-parallel-replicas
# no-parallel: drops the (instance-wide) query condition cache
# no-parallel-replicas: the query condition cache is populated per replica

# Tests the query condition cache for a condition involving the current time when only that part of
# the condition is moved to PREWHERE, e.g. `WHERE time >= today() - 100 AND startsWith(s, 'a')` with
# `time >= today() - 100` in PREWHERE: the granules with old rows are cached and skipped by the next
# query, and the results stay correct.
#
# Like 04931, the test runs in a retry loop: the derived cache key intentionally rotates once per
# grid cell (once per day for a `today() - 100` constant), so a run straddling midnight can lose the
# entry between the priming query and the probing query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# enable_analyzer = 1: the query condition cache only works with the analyzer (query_info has no
# filter DAG without it), like in the other query_condition_cache tests.
# move_all_conditions_to_prewhere = 0: move only the cheap `time` condition to PREWHERE, not the
# condition on the large column `s` (wide parts: the column sizes are known per column).
settings="use_query_condition_cache = true, use_query_condition_cache_for_time_conditions = true, enable_analyzer = 1,
    optimize_move_to_prewhere = 1, move_all_conditions_to_prewhere = 0, use_top_k_dynamic_filtering = 0"

# A single part with three kinds of granules: old rows (filtered by PREWHERE), recent rows not
# matching the condition on `s` (filtered by WHERE only), and recent rows matching it.
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS tab;
    CREATE TABLE tab (time DateTime, x UInt64, s String) ENGINE = MergeTree ORDER BY x
        SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    INSERT INTO tab
    SELECT
        if(number < 100_000, toDateTime('2000-01-01 00:00:00') + (number % 86400), now() - (number % 3600)),
        number,
        concat(if(number < 200_000, 'b', 'a'), randomPrintableASCII(100))
    FROM numbers(300_000)
    SETTINGS max_insert_threads = 1, max_block_size = 300_000, min_insert_block_size_rows = 300_000, min_insert_block_size_bytes = 0;
"

query="SELECT sum(x) FROM tab WHERE time >= today() - 100 AND startsWith(s, 'a') SETTINGS ${settings}"

echo -n "only the time condition is in PREWHERE: "
${CLICKHOUSE_CLIENT} --query "EXPLAIN actions = 1 ${query}" | grep -cE "Prewhere filter column: +time >= "

for _ in 1 2 3; do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"
    ${CLICKHOUSE_CLIENT} --query "${query} FORMAT Null -- prime"
    entries=$(${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.query_condition_cache")
    result=$(${CLICKHOUSE_CLIENT} --query "${query} -- probe")
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    # The granules with old rows (a third of all granules) are skipped.
    hits=$(${CLICKHOUSE_CLIENT} --query "
        SELECT ProfileEvents['QueryConditionCacheHits'] > 0
            AND toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND type = 'QueryFinish'
            AND current_database = currentDatabase()
            AND endsWith(query, '-- probe')
        ORDER BY event_time_microseconds DESC
        LIMIT 1")
    if [ "${entries} ${hits}" == "1 1" ]; then
        break
    fi
done
echo "entries: ${entries}, hits: ${hits}"

echo -n "same result as without the cache: "
${CLICKHOUSE_CLIENT} --query "
    SELECT ${result} = (SELECT sum(x) FROM tab WHERE time >= today() - 100 AND startsWith(s, 'a') SETTINGS use_query_condition_cache = 0)"

${CLICKHOUSE_CLIENT} --query "DROP TABLE tab"
