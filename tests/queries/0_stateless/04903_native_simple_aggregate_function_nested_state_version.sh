#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SimpleAggregateFunction` stores a second copy of its argument type in its custom name. When the
# argument contains an aggregate state below an Array, both that copy and the storage type must be
# rewritten for an old peer; otherwise the `Native` header says version 1 while the payload is
# encoded as version 0.

table="saf_nested_versioned_${CLICKHOUSE_DATABASE}"
type="SimpleAggregateFunction(anyLast, Array(AggregateFunction(1, sumMap, Array(UInt8), Array(UInt8))))"

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE IF EXISTS $table;
    CREATE TABLE $table (x $type) ENGINE = AggregatingMergeTree ORDER BY tuple();
    INSERT INTO $table SELECT CAST([sumMapState([1::UInt8], [2::UInt8])], '$type');"

blob="${CLICKHOUSE_TMP}/04903_old.native"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&client_protocol_version=54337" \
    --data-binary "SELECT x FROM ${CLICKHOUSE_DATABASE}.${table} FORMAT Native" > "$blob"

if LC_ALL=C grep -aqF 'SimpleAggregateFunction(anyLast, Array(AggregateFunction(1,' "$blob"; then
    echo 'old peer announced version 1'
else
    echo 'old peer announced version 0'
fi

rm -f "$blob"
$CLICKHOUSE_CLIENT --query "DROP TABLE $table"
