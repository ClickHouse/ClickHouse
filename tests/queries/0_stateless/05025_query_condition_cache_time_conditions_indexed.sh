#!/usr/bin/env bash
# Tags: no-parallel, no-parallel-replicas
# no-parallel: drops the (instance-wide) query condition cache
# no-parallel-replicas: the query condition cache is populated per replica

# Tests the index-analysis write path of the query condition cache for conditions involving the
# current time (issue #115504): with `time` in the primary key, the granules that only hold old rows
# are excluded by index analysis (not by the WHERE/PREWHERE filter transforms - they never read
# those granules), so the cache entries for them can only have been written by `ReadFromMergeTree`
# under the skip-index-profiled hash of the derived deterministic condition. A second query proves
# the reuse: it probes the cache before index analysis runs and reports `QueryConditionCacheHits`.
#
# The test runs in a retry loop: the cache key of the grid-aligned constant (`today() - 100`)
# intentionally rotates once per grid cell (once per day here), so a test that straddles midnight
# can lose the cache entry between the priming query and the probing query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# enable_analyzer = 1: the query condition cache only works with the analyzer (query_info has no
# filter DAG without it), like in the other query_condition_cache tests.
settings="use_query_condition_cache = true, use_query_condition_cache_for_time_conditions = true, enable_analyzer = 1"

# A single part ordered by `time`, mixing 'old' rows (which no current-time condition matches, and
# which fill whole granules of their own at the front of the part) with recent rows. A separate
# all-old part would be pruned by part-level statistics before the query condition cache ever sees
# it; a part that spans both ranges can only be pruned granule-wise.
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS tab;
    CREATE TABLE tab (time DateTime, x UInt64) ENGINE = MergeTree ORDER BY time
        SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 8192;
    INSERT INTO tab
    SELECT if(number < 1_000_000, toDateTime('2000-01-01 00:00:00') + (number % 86400), now() - (number % 3600)), number
    FROM numbers(2_000_000)
    SETTINGS max_insert_threads = 1, max_block_size = 2_000_000, min_insert_block_size_rows = 2_000_000, min_insert_block_size_bytes = 0;
"

function scenario()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CONDITION CACHE"

    local query="SELECT sum(x) FROM tab WHERE time >= today() - 100 SETTINGS ${settings} FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "${query} -- prime indexed"
    # The priming query reads no granule without matching rows (index analysis pruned them all),
    # so any cache entry proves the index-analysis write path fired.
    local entries
    entries=$(${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.query_condition_cache")

    ${CLICKHOUSE_CLIENT} --query "${query} -- probe indexed"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    local hits
    hits=$(${CLICKHOUSE_CLIENT} --query "
        SELECT ProfileEvents['QueryConditionCacheHits'] > 0
            AND toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND type = 'QueryFinish'
            AND current_database = currentDatabase()
            AND endsWith(query, '-- probe indexed')
        ORDER BY event_time_microseconds DESC
        LIMIT 1")

    echo "${entries} ${hits}"
}

# Retried in case the priming and the probing query straddle a midnight (see above).
for _ in 1 2 3; do
    result=$(scenario)
    if [ "${result}" == "1 1" ]; then
        break
    fi
done
echo "indexed: ${result}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE tab"
