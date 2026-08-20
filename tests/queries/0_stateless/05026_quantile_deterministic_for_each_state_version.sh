#!/usr/bin/env bash
set -e -o pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE=quantile_deterministic_foreach_state_version
trap '$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE" >/dev/null 2>&1 || true' EXIT

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
