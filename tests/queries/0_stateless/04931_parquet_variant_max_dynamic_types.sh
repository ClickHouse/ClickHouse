#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An unshredded variant column carrying more distinct types than `max_dynamic_types` (32 by
# default): 68 decimals, one per valid (precision, scale) pair of the three variant decimal type
# ids. Every scale is a distinct ClickHouse type, so the types past the limit have to go into the
# Dynamic shared variant. The unscaled value is 12345 in every row, so a scale dropped on the way
# into the column shows up as 12345 instead of 0.00012345 and friends.
DATA_FILE=$CUR_DIR/data_parquet/04931_variant_max_dynamic_types.parquet

echo "-- every row keeps its own decimal type"
${CLICKHOUSE_LOCAL} --query="
    SELECT dynamicType(v) AS type, toString(v) AS value
    FROM file('${DATA_FILE}', Parquet)"

echo "-- distinct types read back"
${CLICKHOUSE_LOCAL} --query="
    SELECT uniqExact(dynamicType(v)), uniqExact(toString(v))
    FROM file('${DATA_FILE}', Parquet)"

echo "-- types beyond the limit live in the shared variant"
${CLICKHOUSE_LOCAL} --query="
    SELECT countIf(isDynamicElementInSharedData(v)) > 0
    FROM file('${DATA_FILE}', Parquet)"
