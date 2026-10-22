#!/usr/bin/env bash

# Deserialization of CrossTab-family aggregate function states (`contingency`, `cramersV`,
# `cramersVBiasCorrected`, `theilsU`) must validate that the counts form a valid contingency table:
# the joint counts are positive, the value counts are exactly the marginal sums of the pair counts,
# and the pair counts sum up to the total count. Otherwise finalization of a forged state
# (e.g. constructed by `CAST` from `String`) could fail an assertion or divide by zero.


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

# The exact query found by the fuzzer: a window `argMin` state reinterpreted as a `contingency`
# state. It previously failed the assertion `phi_squared > -1e-4`.
expect_corrupted_data 'fuzzer query' "
    SELECT round(roundtrip, 2147483647) AS roundtrip, abs(direct - roundtrip) < 1e-9, round(direct) AS direct FROM (SELECT finalizeAggregation(st_win) AS direct, finalizeAggregation(CAST(CAST(st_win, 'String'), 'AggregateFunction(contingency, UInt8, UInt8)')) AS roundtrip FROM (SELECT argMinState(toUInt8(number % 10), *) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS st_win FROM numbers(10000) ORDER BY number DESC NULLS FIRST LIMIT 9223372036854775807))"

# A simplified version of the fuzzer query: the same foreign state built by a plain aggregation.
expect_corrupted_data 'foreign state' "
    SELECT finalizeAggregation(CAST(CAST(argMinState(toUInt8(number % 10), number), 'String'), 'AggregateFunction(contingency, UInt8, UInt8)'))
    FROM numbers(10000)"

# The following byte strings are based on the serialization of `contingencyState(toUInt8(1), toUInt8(2))`
# over two rows: total count 2, then three hash maps (count_a, count_b, count_ab), each serialized as
# a varint size followed by (key, value) pairs.

# A genuine state: deserializes and finalizes normally.
$CLICKHOUSE_LOCAL --query "
    SELECT finalizeAggregation(CAST(unhex('0200000000000000014769A5692A310962020000000000000001D2D33DBDEA4FD1AC0200000000000000014769A5692A310962D2D33DBDEA4FD1AC0200000000000000'), 'AggregateFunction(contingency, UInt8, UInt8)'))"

# The total count is changed from 2 to 3: it does not match the sum of the pair counts.
expect_corrupted_data 'total count mismatch' "
    SELECT finalizeAggregation(CAST(unhex('0300000000000000014769A5692A310962020000000000000001D2D33DBDEA4FD1AC0200000000000000014769A5692A310962D2D33DBDEA4FD1AC0200000000000000'), 'AggregateFunction(contingency, UInt8, UInt8)'))"

# The count of the first value is changed from 2 to 3: it does not match the marginal sum of the pair counts.
expect_corrupted_data 'marginal count mismatch' "
    SELECT finalizeAggregation(CAST(unhex('0200000000000000014769A5692A310962030000000000000001D2D33DBDEA4FD1AC0200000000000000014769A5692A310962D2D33DBDEA4FD1AC0200000000000000'), 'AggregateFunction(contingency, UInt8, UInt8)'))"

# The pair count is changed from 2 to 0: pair counts must be positive.
expect_corrupted_data 'zero pair count' "
    SELECT finalizeAggregation(CAST(unhex('0200000000000000014769A5692A310962020000000000000001D2D33DBDEA4FD1AC0200000000000000014769A5692A310962D2D33DBDEA4FD1AC0000000000000000'), 'AggregateFunction(contingency, UInt8, UInt8)'))"

# All functions of the family share the state format and the validation.
for func in cramersV cramersVBiasCorrected theilsU
do
    expect_corrupted_data "$func" "
        SELECT finalizeAggregation(CAST(unhex('0300000000000000014769A5692A310962020000000000000001D2D33DBDEA4FD1AC0200000000000000014769A5692A310962D2D33DBDEA4FD1AC0200000000000000'), 'AggregateFunction($func, UInt8, UInt8)'))"
done

# Genuine states still survive a roundtrip through `String` and finalize to the same results.
$CLICKHOUSE_LOCAL --query "
    SELECT
        finalizeAggregation(st_c) = finalizeAggregation(CAST(CAST(st_c, 'String'), 'AggregateFunction(contingency, UInt16, UInt16)')),
        finalizeAggregation(st_v) = finalizeAggregation(CAST(CAST(st_v, 'String'), 'AggregateFunction(cramersV, UInt16, UInt16)')),
        finalizeAggregation(st_b) = finalizeAggregation(CAST(CAST(st_b, 'String'), 'AggregateFunction(cramersVBiasCorrected, UInt16, UInt16)')),
        finalizeAggregation(st_u) = finalizeAggregation(CAST(CAST(st_u, 'String'), 'AggregateFunction(theilsU, UInt16, UInt16)'))
    FROM
    (
        SELECT
            contingencyState(toUInt16(number % 17), toUInt16(number % 9)) AS st_c,
            cramersVState(toUInt16(number % 17), toUInt16(number % 9)) AS st_v,
            cramersVBiasCorrectedState(toUInt16(number % 17), toUInt16(number % 9)) AS st_b,
            theilsUState(toUInt16(number % 17), toUInt16(number % 9)) AS st_u
        FROM numbers(1000)
    )"
