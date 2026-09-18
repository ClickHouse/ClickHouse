#!/usr/bin/env bash

# Deserialization of an `analysisOfVariance` aggregate function state must validate that its three
# internal vectors (group sums, group squared sums and group counts) have equal sizes. `merge` and
# finalization index all three vectors by the size of the first one, so a forged state (e.g.
# constructed by `CAST` from `String`) with mismatched sizes could otherwise read out of bounds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function expect_corrupted_data()
{
    local name="$1"
    local query="$2"
    $CLICKHOUSE_LOCAL --query "$query" 2>&1 \
        | grep -q -F 'CORRUPTED_DATA' && echo "OK $name" || echo "FAIL $name"
}

# The state is serialized as three vectors, each a varint size followed by that many values:
# the group sums `xs1` (Float64), the group squared sums `xs2` (Float64) and the group counts `ns`
# (UInt64). A genuine state produced by `add`/`merge` keeps the three sizes equal.

# xs1 has two elements while xs2 has none and ns has one: finalization would read past `xs2`/`ns`.
expect_corrupted_data 'xs1 longer than xs2 and ns' \
    "SELECT finalizeAggregation(CAST(unhex('02000000000000f03f000000000000004000010100000000000000'), 'AggregateFunction(analysisOfVariance, Float64, UInt64)'))"

# xs1 and xs2 have one element while ns has none: finalization would read past `ns`.
expect_corrupted_data 'ns shorter than xs1 and xs2' \
    "SELECT finalizeAggregation(CAST(unhex('01000000000000f03f01000000000000f03f00'), 'AggregateFunction(analysisOfVariance, Float64, UInt64)'))"

# A genuine state still survives a roundtrip through `String` and finalizes to the same result.
$CLICKHOUSE_LOCAL --query "
    SELECT finalizeAggregation(st) = finalizeAggregation(CAST(CAST(st, 'String'), 'AggregateFunction(analysisOfVariance, Float64, UInt64)'))
    FROM (SELECT analysisOfVarianceState(number::Float64, number % 3) AS st FROM numbers(30))"
