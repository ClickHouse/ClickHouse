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

# A run that never spilled would satisfy the read_rows assertion trivially, so require the spill first.
SPILLED=$(${CLICKHOUSE_CLIENT} --print-profile-events -q "${QUERY}" 2>&1 | grep -c "ExternalAggregationMerge")
if [[ "$SPILLED" -lt 1 ]]; then
    echo "spill_did_not_happen"
else
    echo "spill_happened"
fi

SUMMARY=$(${CLICKHOUSE_CURL} -sS -i "${CLICKHOUSE_URL}" --data-binary "${QUERY}" 2>&1 | grep -i '^X-ClickHouse-Summary:' | head -1)
READ_ROWS=$(echo "$SUMMARY" | grep -oP '"read_rows":"\K[0-9]+')

if [[ "$READ_ROWS" == "100000" ]]; then
    echo "read_rows_correct"
else
    echo "read_rows_wrong:$READ_ROWS"
fi
