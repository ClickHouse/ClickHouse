#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SimpleAggregateFunction` keeps its own copy of the nested state type, and that copy is what both the
# printed type name and the binary type encoding are made of. When `NativeWriter` downgrades the state
# to the version of an older peer, the copy has to be downgraded with it - otherwise the block announces
# `AggregateFunction(1, ...)` while carrying a version 0 payload, which an old peer cannot even parse and
# a version 0 reader would deserialize one version too high.
# https://github.com/ClickHouse/ClickHouse/pull/112052

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS saf_versioned;
CREATE TABLE saf_versioned (x SimpleAggregateFunction(anyLast, AggregateFunction(1, sumMap, Array(UInt8), Array(UInt8))))
ENGINE = AggregatingMergeTree ORDER BY tuple();
INSERT INTO saf_versioned SELECT initializeAggregation('sumMapState', [1::UInt8], [2::UInt8]);
"

query="SELECT x FROM ${CLICKHOUSE_DATABASE}.saf_versioned FORMAT Native"

announced_version()
{
    local blob="$1"
    if ! LC_ALL=C grep -aqF 'SimpleAggregateFunction(anyLast,' "$blob"; then
        echo "the wrapper was lost"
    elif LC_ALL=C grep -aqF 'AggregateFunction(1,' "$blob"; then
        echo "version 1"
    else
        echo "no version"
    fi
}

# A peer that predates aggregate function state versioning gets version 0 states, so the type it is told
# about must not mention a version either.
old_blob="${CLICKHOUSE_TMP}/04695_old.native"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&client_protocol_version=54337" --data-binary "$query" > "$old_blob"
echo "old peer: $(announced_version "$old_blob")"

# `FORMAT Native` with no negotiated revision at all has no peer to derive a version for: the stream
# is self-describing, so the version pinned on the type survives into it and is announced as such.
default_blob="${CLICKHOUSE_TMP}/04695_default.native"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}" --data-binary "$query" > "$default_blob"
echo "no revision: $(announced_version "$default_blob")"

# A current peer gets version 1 states and is told so.
new_blob="${CLICKHOUSE_TMP}/04695_new.native"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&client_protocol_version=54489" --data-binary "$query" > "$new_blob"
echo "current peer: $(announced_version "$new_blob")"

# The state itself is unchanged by any of this.
$CLICKHOUSE_CLIENT -m -q "
SELECT finalizeAggregation(x) FROM ${CLICKHOUSE_DATABASE}.saf_versioned;
DROP TABLE ${CLICKHOUSE_DATABASE}.saf_versioned;
"

rm -f "$old_blob" "$default_blob" "$new_blob"
