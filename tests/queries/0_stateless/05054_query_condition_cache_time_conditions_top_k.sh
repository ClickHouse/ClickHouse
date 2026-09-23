#!/usr/bin/env bash
# Tags: no-parallel, no-parallel-replicas
# no-parallel: drops the (instance-wide) query condition cache
# no-parallel-replicas: the query condition cache is populated per replica

# Tests that a condition involving the current time is derived (and hence cached) also for TopK
# reads, i.e. `ORDER BY ... LIMIT n` with dynamic filtering, where the internal `__topKFilter`
# function is folded into the storage filter as `and(__topKFilter(...), time >= ...)`.
#
# Like 04931, the test runs in a retry loop: the derived cache key intentionally rotates once per
# grid cell (once per day for a `today() - 100` constant), so a run straddling midnight can lose the
# entry between the priming query and the probing query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# enable_analyzer = 1: the query condition cache only works with the analyzer (query_info has no
# filter DAG without it), like in the other query_condition_cache tests.
settings="use_query_condition_cache = true, use_query_condition_cache_for_time_conditions = true, use_query_condition_cache_for_top_k = true, use_top_k_dynamic_filtering = true, enable_analyzer = 1"

# The same shape as in 04931: a single part mixing 'old' rows (matched by no current-time condition)
# with recent rows, with the old rows filling whole granules of their own so that they can be pruned
# granule-wise by the query condition cache.
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
    local query_body="$2"

    ${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"

    local query="${query_body} SETTINGS ${settings}, optimize_move_to_prewhere = false FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "${query} -- prime ${name}"
    local entries
    entries=$(${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.query_condition_cache")

    ${CLICKHOUSE_CLIENT} --query "${query} -- probe ${name}"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    local hits
    hits=$(${CLICKHOUSE_CLIENT} --query "
        SELECT ProfileEvents['QueryConditionCacheHits'] > 0
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
        result=$(scenario "$name" "$2")
        if [ "${result}" == "1 1" ]; then
            break
        fi
    done
    echo "${name}: ${result}"
}

# `ORDER BY time DESC` is not the table's order, so the read goes through TopK dynamic filtering and
# the derived condition must look through the internal `__topKFilter` node.
scenario_with_retries "top k" "SELECT x FROM tab WHERE time >= today() - 100 ORDER BY time DESC LIMIT 5"

# A TopK read must also be able to reuse the entries primed by a plain `SELECT ... WHERE`, which are
# keyed on the derived condition of the predicate alone (the predicate-only reuse hash).
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"
for _ in 1 2 3; do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT sum(x) FROM tab WHERE time >= today() - 100
        SETTINGS ${settings}, optimize_move_to_prewhere = false FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT x FROM tab WHERE time >= today() - 100 ORDER BY time DESC LIMIT 5
        SETTINGS ${settings}, optimize_move_to_prewhere = false FORMAT Null -- probe reuse"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    reuse=$(${CLICKHOUSE_CLIENT} --query "
        SELECT ProfileEvents['QueryConditionCacheHits'] > 0
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND type = 'QueryFinish'
            AND current_database = currentDatabase()
            AND endsWith(query, '-- probe reuse')
        ORDER BY event_time_microseconds DESC
        LIMIT 1")
    if [ "${reuse}" == "1" ]; then
        break
    fi
    ${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"
done
echo "plain WHERE entries reused by top k: ${reuse}"

# With the setting disabled, a TopK read of a condition involving the current time is not cached.
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"
${CLICKHOUSE_CLIENT} --query "
    SELECT x FROM tab WHERE time >= today() - 100 ORDER BY time DESC LIMIT 5
    SETTINGS use_query_condition_cache = true, use_query_condition_cache_for_time_conditions = false,
        use_top_k_dynamic_filtering = true, enable_analyzer = 1, optimize_move_to_prewhere = false FORMAT Null"
echo -n "disabled, entries cached: "
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.query_condition_cache"

${CLICKHOUSE_CLIENT} --query "DROP TABLE tab"
