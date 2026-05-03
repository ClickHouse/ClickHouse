#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: relies on temporary files for external aggregation

# Test: exercises `SourceFromNativeStream::getReadProgress` returning nullopt.
# Covers: src/Processors/Transforms/AggregatingTransform.cpp:143 — without this override,
#         rows read back from spilled native-format temporary files would be added to
#         read_rows progress via the ISource auto-progress mechanism, producing
#         read_rows > input rows when GROUP BY spills to disk.
# Mutation guard: removing the line `getReadProgress() override { return std::nullopt; }`
#         makes read_rows reported in X-ClickHouse-Summary exceed the input row count.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY="SELECT count() FROM (SELECT number FROM numbers(100000) GROUP BY number) SETTINGS max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0"

# Confirm the path is actually exercised: external aggregation must spill to disk
SPILLED=$(${CLICKHOUSE_CLIENT} --print-profile-events -q "${QUERY}" 2>&1 | grep -c "ExternalAggregationMerge")
if [[ "$SPILLED" -lt 1 ]]; then
    echo "spill_did_not_happen"
else
    echo "spill_happened"
fi

# Get read_rows from HTTP X-ClickHouse-Summary. Should equal input row count (100000).
SUMMARY=$(${CLICKHOUSE_CURL} -sS -i "${CLICKHOUSE_URL}" --data-binary "${QUERY}" 2>&1 | grep -i '^X-ClickHouse-Summary:' | head -1)
READ_ROWS=$(echo "$SUMMARY" | grep -oP '"read_rows":"\K[0-9]+')

if [[ "$READ_ROWS" == "100000" ]]; then
    echo "read_rows_correct"
else
    echo "read_rows_wrong:$READ_ROWS"
fi
