#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `FORMAT Native` writes at the revision of the peer, which is 0 unless the caller passes
# `client_protocol_version`. A versioned aggregate function state is then serialized at version 0, so the
# type the block announces must say version 0 as well. For a state nested inside `SimpleAggregateFunction`
# the announced type comes from the custom name, which keeps its own copy of the argument types: it used
# to still say `AggregateFunction(1, ...)` while the payload was written at version 0, so the reader
# deserialized a version 1 state out of a version 0 payload.
# https://github.com/ClickHouse/ClickHouse/pull/112052

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS saf_agg_downgrade;
CREATE TABLE saf_agg_downgrade (x SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt8), Array(UInt8))))
ENGINE = AggregatingMergeTree ORDER BY tuple();
INSERT INTO saf_agg_downgrade SELECT initializeAggregation('sumMapState', [1::UInt8], [2::UInt8]);
"

echo '-- type name on the wire'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT x FROM saf_agg_downgrade FORMAT Native" > "${CLICKHOUSE_TMP}/04670.native"
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(x), finalizeAggregation(x) FROM file('${CLICKHOUSE_TMP}/04670.native', 'Native')"

echo '-- binary encoded type name on the wire'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&output_format_native_encode_types_in_binary_format=1" \
    -d "SELECT x FROM saf_agg_downgrade FORMAT Native" > "${CLICKHOUSE_TMP}/04670_binary.native"
$CLICKHOUSE_LOCAL -q "SELECT toTypeName(x), finalizeAggregation(x) FROM file('${CLICKHOUSE_TMP}/04670_binary.native', 'Native')
    SETTINGS input_format_native_decode_types_in_binary_format = 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE saf_agg_downgrade"

rm -f "${CLICKHOUSE_TMP}/04670.native" "${CLICKHOUSE_TMP}/04670_binary.native"
