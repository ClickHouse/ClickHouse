#!/usr/bin/env bash
set -e -o pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE=quantile_deterministic_foreach_state_version
MERGE_TABLE=quantile_deterministic_merge_state_version
trap '$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE; DROP TABLE IF EXISTS $MERGE_TABLE" >/dev/null 2>&1 || true' EXIT

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS $TABLE;
    CREATE TABLE $TABLE
    (
        id UInt8,
        state AggregateFunction(quantileDeterministicForEach, Array(UInt64), Array(UInt64))
    )
    ENGINE = MergeTree
    ORDER BY id;
"

# The nested quantile state uses version 0 by default, so this catches -ForEach dropping the
# explicit version while serializing a state into a column pinned to version 1.
$CLICKHOUSE_CLIENT -q "SELECT toTypeName(quantileDeterministicForEachState([number], [number])) FROM numbers(1)"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO $TABLE
    SELECT 1, quantileDeterministicForEachState([number], [number])
    FROM numbers(5);
"

$CLICKHOUSE_CLIENT -q "SELECT quantileDeterministicForEachMerge(state) FROM $TABLE"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS $MERGE_TABLE;
    CREATE TABLE $MERGE_TABLE
    (
        state AggregateFunction(quantileDeterministicMerge, AggregateFunction(quantileDeterministic, UInt64, UInt64))
    )
    ENGINE = MergeTree
    ORDER BY tuple();
"

$CLICKHOUSE_CLIENT -q "SELECT type FROM system.columns WHERE database = currentDatabase() AND table = '$MERGE_TABLE' AND name = 'state'"
$CLICKHOUSE_CLIENT -q "
    SELECT toTypeName(quantileDeterministicMergeState(state))
    FROM (SELECT quantileDeterministicState(number, number) AS state FROM numbers(1))
"
$CLICKHOUSE_CLIENT -q "
    SELECT toTypeName(quantileDeterministicMergeOrDefaultState(state))
    FROM (SELECT quantileDeterministicState(number, number) AS state FROM numbers(1))
"
