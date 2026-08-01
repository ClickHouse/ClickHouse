#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The state version of an `AggregateFunction` nested in a `SimpleAggregateFunction` is a part of the
# printed type name, and the receiver reads it off that name. The version the states are serialized
# at must therefore be the one the name announces, also when the block is written for a peer that
# negotiated a lower revision - `FORMAT Native` writes with revision 0.
# https://github.com/ClickHouse/ClickHouse/pull/112052

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS saf_state;
CREATE TABLE saf_state (x SimpleAggregateFunction(anyLast, AggregateFunction(sumMap, Array(UInt8), Array(UInt8)))) ENGINE = AggregatingMergeTree ORDER BY tuple();
INSERT INTO saf_state SELECT initializeAggregation('sumMapState', [3::UInt8], [4::UInt8]);
"

$CLICKHOUSE_CLIENT -q "SELECT x FROM saf_state FORMAT Native" > "${CLICKHOUSE_TMP}/saf_state.native"

# The announced type and the states in the payload have to agree, so reading the file back works.
$CLICKHOUSE_LOCAL -q "DESCRIBE file('${CLICKHOUSE_TMP}/saf_state.native', Native)" | cut -f 1,2
$CLICKHOUSE_LOCAL -q "SELECT finalizeAggregation(x) FROM file('${CLICKHOUSE_TMP}/saf_state.native', Native)"

$CLICKHOUSE_CLIENT -q "DROP TABLE saf_state"
rm -f "${CLICKHOUSE_TMP}/saf_state.native"
