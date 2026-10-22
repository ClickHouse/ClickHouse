#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# LeakSanitizer's at-exit check makes `clickhouse local` abort when a state is leaked.
# Without ASan there is nothing to detect, so only the results are checked.
asan=$(${CLICKHOUSE_LOCAL} --query "SELECT count() FROM system.build_options
    WHERE name = 'CXX_FLAGS' AND position('sanitize=address' IN value)")

# The sorting prefix must be SHORTER than the GROUP BY key: that is what selects `group_by_key`
# mode, and the processor name is identical in both modes so it cannot tell them apart. Parallel
# replicas add a second `Order:` line from the coordinator, so pin the setting it depends on.
query="
    CREATE TABLE data_04834 (parent_key Int, child_key Int, value Float64)
        ENGINE = MergeTree() ORDER BY parent_key;
    INSERT INTO data_04834 SELECT number % 10, number % 3, number FROM numbers(1000);

    SELECT trimBoth(replaceRegexpAll(explain, '__table1.', ''))
    FROM (
        EXPLAIN actions = 1
        SELECT parent_key, child_key, quantileDD(0.01, 0.5)(value)
        FROM data_04834 GROUP BY parent_key, child_key
        SETTINGS max_threads = 1, optimize_aggregation_in_order = 1, enable_parallel_replicas = 0
    )
    WHERE explain LIKE '%Order:%';

    SELECT parent_key, child_key, round(quantileDD(0.01, 0.5)(value), 2)
    FROM data_04834 GROUP BY parent_key, child_key
    ORDER BY parent_key, child_key
    SETTINGS max_threads = 1, optimize_aggregation_in_order = 1, enable_parallel_replicas = 0;"

# quantileDD's state owns heap allocations, so a leaked state is visible to LeakSanitizer.
out=$(${CLICKHOUSE_LOCAL} --path "${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}" \
    --multiquery --query "SET explain_query_plan_default = 'legacy'; ${query}" 2>&1)
rc=$?
rm -rf "${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"

if [ "$asan" = "1" ] && { [ "$rc" != "0" ] || echo "$out" | grep -q "LeakSanitizer"; }; then
    echo "LEAKED: rc=$rc"
    echo "$out" | grep -aE "LeakSanitizer|SUMMARY: AddressSanitizer" | head -2
else
    echo "$out"
fi
