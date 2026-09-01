#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# LeakSanitizer's at-exit check aborts `clickhouse local` when a state is leaked. Without ASan
# there is nothing to detect, so only the results are checked.
asan=$(${CLICKHOUSE_LOCAL} --query "SELECT count() FROM system.build_options
    WHERE name = 'CXX_FLAGS' AND position('sanitize=address' IN value)")

# `serialize` walks a map, so the repeated key below cannot come from a writer. The nested
# `groupArrayIntersect` holds a plain `HashSet`, so `create` takes heap before the rejection.
blob='020161010301000000000000000300000000000000020000000000000001610103010000000000000003000000000000000200000000000000'
out=$(${CLICKHOUSE_LOCAL} --path "${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}" --query \
    "SELECT finalizeAggregation(CAST(unhex('${blob}'),
        'AggregateFunction(groupArrayIntersectMap, Map(String, Array(UInt64)))'));" 2>&1)
rc=$?
rm -rf "${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"

# A run that never reached the rejection would leak nothing, so count it rather than assume it.
echo "$out" | grep -ac INCORRECT_DATA

# The rejection makes an error code expected here, and the runner may route the report to a file,
# so the abort is the signal. Both are checked because either alone can be the visible one.
if [ "$asan" = "1" ] && { [ "$rc" -ge 128 ] || echo "$out" | grep -q "LeakSanitizer"; }; then
    echo "LEAKED: rc=$rc"
    echo "$out" | grep -aE "SUMMARY: AddressSanitizer" | head -1
fi
