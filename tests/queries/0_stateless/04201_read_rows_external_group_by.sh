#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# Tag no-fasttest: external aggregation needs temporary files
# Tag no-random-settings: randomized group_by_two_level_threshold*, optimize_aggregation_in_order and
#                         the max_bytes_ratio_before_external_group_by family change whether and how
#                         the aggregation spills, which is the path this asserts

# When GROUP BY spills, the aggregated states are re-read from native-format temporary files. Those
# re-read rows are not input rows, so the source that reads them reports no read progress of its own
# (SourceFromNativeStream::getReadProgress in AggregatingTransform.cpp:150 returns nullopt). Reported
# read_rows must therefore stay equal to the number of rows actually read from the input, not grow by
# the spilled rows read back during the merge.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY="SELECT count() FROM (SELECT number FROM numbers(100000) GROUP BY number) SETTINGS max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0"

QID="04201_read_rows_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query_id "$QID" -q "${QUERY}" > /dev/null

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# Both facts come from that one execution: the query produces exactly group_by_two_level_threshold
# keys, so whether it spills is decided per run, and a run that did not spill satisfies the
# read_rows assertion trivially. LIMIT 1 rather than an aggregate, so that no row at all leaves both
# values empty and fails loudly.
read -r SPILLED READ_ROWS <<<"$(${CLICKHOUSE_CLIENT} -q "
    SELECT ProfileEvents['ExternalAggregationMerge'], read_rows
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
        AND query_id = '${QID}' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1")"

if [[ "${SPILLED:-0}" -lt 1 ]]; then
    echo "spill_did_not_happen"
else
    echo "spill_happened"
fi

if [[ "$READ_ROWS" == "100000" ]]; then
    echo "read_rows_correct"
else
    echo "read_rows_wrong:$READ_ROWS"
fi
