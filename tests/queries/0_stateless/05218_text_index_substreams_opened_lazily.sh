#!/usr/bin/env bash
# Tags: no-object-storage
# no-object-storage: the oracle is ProfileEvents['FileOpen'], which counts local file opens only.
# Random settings limits: min_bytes_to_use_mmap_io=(1048576, None)
# min_bytes_to_use_mmap_io: below the file sizes here it makes every read go through the mmap cache,
# which does not count as a file open at all, so both counts collapse to 0.

# A text index granule that answers from its caches reads none of its substreams, so their data
# files must not be opened.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PARTS=30

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_text_lazy_substreams;
    CREATE TABLE t_text_lazy_substreams (id UInt64, val UInt64, msg String,
        INDEX idx msg TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 8, min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 100000000,
        ratio_of_defaults_for_sparse_serialization = 1, prewarm_mark_cache = 0;

    SYSTEM STOP MERGES t_text_lazy_substreams;
"

# One part per insert.  'everypart' is in every part, 'onepart' only in the last one.
for i in $(seq 1 $PARTS); do
    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO t_text_lazy_substreams
        SELECT number + $i * 100, number, concat('everypart w', toString(number % 3), if($i = $PARTS AND number = 5, ' onepart', ''))
        FROM numbers(8)"
done

SETTINGS_COMMON="use_query_condition_cache = 0, use_query_cache = 0, use_text_index_negative_tokens_cache = 1,
    use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, load_marks_asynchronously = 0,
    enable_parallel_replicas = 0, max_threads = 1"

# A per-query postings cache can never hit, so the in-range query below always reads its postings
# and the third one, which must not, needs the global cache instead.  Reading the index instead
# of the column keeps the third query's budget down to one open per part, which is what makes its
# narrower threshold measurable.
SETTINGS="$SETTINGS_COMMON, use_text_index_postings_cache = 0"
SETTINGS_POSTINGS_CACHE="$SETTINGS_COMMON, use_text_index_postings_cache = 1,
    query_plan_direct_read_from_text_index = 1"

SELECTIVE="SELECT sum(val) FROM t_text_lazy_substreams WHERE hasToken(msg, 'onepart') SETTINGS $SETTINGS"
IN_RANGE="SELECT sum(val) FROM t_text_lazy_substreams WHERE hasToken(msg, 'everypart') SETTINGS $SETTINGS"

# Unmeasured warm-up: the point of the test is the steady state of a repeated search term, where the
# index header and the negative-tokens caches are already populated.
${CLICKHOUSE_CLIENT} -q "$SELECTIVE" > /dev/null
${CLICKHOUSE_CLIENT} -q "$IN_RANGE" > /dev/null

SELECTIVE_ID="05218_selective_${CLICKHOUSE_DATABASE}"
IN_RANGE_ID="05218_in_range_${CLICKHOUSE_DATABASE}"

# The answers are asserted too: a threshold on a counter says nothing if the query stopped
# returning the right rows.
echo -n "selective_answer	"; ${CLICKHOUSE_CLIENT} --query_id "$SELECTIVE_ID" -q "$SELECTIVE"
echo -n "in_range_answer	";  ${CLICKHOUSE_CLIENT} --query_id "$IN_RANGE_ID" -q "$IN_RANGE"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# A threshold, not the exact count: the number of incidental opens is not the property under test.
# The selective query is measured at exactly 1 open, so PARTS / 3 leaves an order of magnitude of
# headroom.  The in-range query is the control: it must stay ABOVE the same threshold, otherwise the
# fixture stopped reaching the index and the first assertion would pass for the wrong reason.
${CLICKHOUSE_CLIENT} -q "
    SELECT
        if(query_id = '$SELECTIVE_ID', 'selective_below_threshold', 'in_range_above_threshold'),
        if(query_id = '$SELECTIVE_ID', ProfileEvents['FileOpen'] < $PARTS / 3, ProfileEvents['FileOpen'] > $PARTS)
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND event_date >= yesterday() AND query_id IN ('$SELECTIVE_ID', '$IN_RANGE_ID')
    ORDER BY query_id DESC"

# The same in-range query once its postings block is in the global postings cache: reading no
# substream then covers the postings stream too.  A threshold again, since that cache is shared with
# concurrently running tests.
POSTINGS_CACHED="SELECT sum(val) FROM t_text_lazy_substreams WHERE hasToken(msg, 'everypart') SETTINGS $SETTINGS_POSTINGS_CACHE"
POSTINGS_CACHED_ID="05218_postings_cached_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "$POSTINGS_CACHED" > /dev/null

echo -n "postings_cached_answer	"; ${CLICKHOUSE_CLIENT} --query_id "$POSTINGS_CACHED_ID" -q "$POSTINGS_CACHED"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} -q "
    SELECT 'postings_cached_below_threshold', ProfileEvents['FileOpen'] < $PARTS + $PARTS / 2
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND event_date >= yesterday() AND query_id = '$POSTINGS_CACHED_ID'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_text_lazy_substreams"
