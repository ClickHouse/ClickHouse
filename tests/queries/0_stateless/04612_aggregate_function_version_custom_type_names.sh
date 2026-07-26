#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Assigning a serialization version to a nested aggregate function replaces the versioned leaf, so the
# type around it is rebuilt. That rebuild must keep custom type names: `Nested(...)` must not degrade
# into `Array(Tuple(...))`, and `SimpleAggregateFunction(f, T)` must not degrade into a plain `T` -
# the latter also changes how AggregatingMergeTree and SummingMergeTree merge the column.

# The name can also sit on the wrapper instead of on the leaf, as in WRAPPED below, where it is the
# Array that carries it while the replaced leaf is one level down.

NESTED="Nested(x AggregateFunction(sumMap, Array(UInt64), Array(UInt64)))"
SIMPLE="SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt64), Array(UInt64)))"
WRAPPED="SimpleAggregateFunction(anyLast, Array(AggregateFunction(sumMap, Array(UInt64), Array(UInt64))))"

# A non-empty state, so that the values below actually exercise the serialization rather than only
# the zero map size an empty state writes.
STATE="sumMapState(CAST([1, 2], 'Array(UInt64)'), CAST([10, 20], 'Array(UInt64)'))"

# The version is assigned by the Native writer and again by the Native reader, so a round trip
# through Native exercises both. The type names and the values must come back unchanged.
$CLICKHOUSE_LOCAL --query "
    SELECT CAST([tuple($STATE)], '$NESTED') AS n, CAST($STATE, '$SIMPLE') AS s, CAST([$STATE], '$WRAPPED') AS w
    FORMAT Native" \
    | $CLICKHOUSE_LOCAL --input-format Native --query "
        SELECT toTypeName(n), toTypeName(s), toTypeName(w),
               finalizeAggregation(n[1].1), finalizeAggregation(s), finalizeAggregation(w[1])
        FROM table FORMAT Vertical"

# The same assignment happens on ATTACH, where the result becomes the column type in the metadata.
$CLICKHOUSE_CLIENT --query "
    SET flatten_nested = 0;

    DROP TABLE IF EXISTS t_agg_version_names;

    CREATE TABLE t_agg_version_names (k UInt32, n $NESTED, s $SIMPLE, w $WRAPPED)
    ENGINE = MergeTree ORDER BY k;

    DETACH TABLE t_agg_version_names;
    ATTACH TABLE t_agg_version_names;

    SELECT name, type FROM system.columns
    WHERE database = currentDatabase() AND table = 't_agg_version_names' AND name != 'k'
    ORDER BY name;

    DROP TABLE t_agg_version_names;"
