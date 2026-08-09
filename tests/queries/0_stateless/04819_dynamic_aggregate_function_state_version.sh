#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A versioned aggregate function state carried inside a `Dynamic` value always keeps state version 0
# unless its type spells a version out explicitly. `Dynamic` announces its nested types itself - via
# its own type lists and via the binary type encoding, which has no version field (the values of the
# shared variant embed their type that way even at rest) - so a version derived from the negotiated
# revision could never reach the reader. The version of an unversioned type therefore must not depend
# on the server's defaults: it stays 0 everywhere, and the bytes of a `Dynamic` column are the same
# for every peer and every medium. https://github.com/ClickHouse/ClickHouse/pull/112052

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS dynamic_qd;
DROP TABLE IF EXISTS dynamic_qd_rt;
CREATE TABLE dynamic_qd (d Dynamic) ENGINE = MergeTree ORDER BY tuple();
-- Enough rows for the reservoir to be thinned out: a state version 1 payload would have a non-zero
-- skip degree to write, which is exactly where the versions differ.
INSERT INTO dynamic_qd SELECT quantileDeterministicState(number, number) FROM numbers(1000000);
INSERT INTO dynamic_qd VALUES (42::UInt8);

-- The nested type keeps its unversioned spelling: state version 0.
SELECT DISTINCT dynamicType(d) FROM dynamic_qd ORDER BY 1;

-- The state is readable in place through the unversioned subcolumn name.
SELECT medianDeterministicMerge(d.\`AggregateFunction(quantileDeterministic, UInt64, UInt64)\`) FROM dynamic_qd;

CREATE TABLE dynamic_qd_rt (d Dynamic) ENGINE = Memory;
"

query="SELECT d FROM ${CLICKHOUSE_DATABASE}.dynamic_qd FORMAT Native"

announced_version()
{
    local blob="$1"
    if LC_ALL=C grep -aqF 'AggregateFunction(1,' "$blob"; then
        echo "version 1"
    else
        echo "no version"
    fi
}

# Feeds a Native blob back to the server and merges the state it carried, to prove the payload is
# really written at the version the announcement implies: a version mismatch either desynchronizes
# the stream or reads a skip degree out of bytes that belong to the next value. Only blobs produced
# with no negotiated revision can go here: `INSERT ... FORMAT Native` parses revision 0 framing, and
# a non-zero revision adds `BlockInfo` in front of each block.
roundtrip()
{
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE dynamic_qd_rt"
    $CLICKHOUSE_CLIENT "$@" -q "INSERT INTO dynamic_qd_rt FORMAT Native"
    $CLICKHOUSE_CLIENT -q "SELECT medianDeterministicMerge(d.\`AggregateFunction(quantileDeterministic, UInt64, UInt64)\`) FROM dynamic_qd_rt"
}

blob="${CLICKHOUSE_TMP}/04819_dynamic.native"

# The nested state does not follow the negotiated revision: a peer that predates the state version,
# no negotiated revision at all, and a current peer are all told the same unversioned nested type.
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&client_protocol_version=54337" --data-binary "$query" > "$blob"
echo "old peer: $(announced_version "$blob")"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&client_protocol_version=54489" --data-binary "$query" > "$blob"
echo "current peer: $(announced_version "$blob")"

# An unversioned announcement carries a version 0 payload.
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}" --data-binary "$query" > "$blob"
echo "no revision: $(announced_version "$blob"), round trip $(roundtrip < "$blob")"

# The binary type encoding of the announcement has no version field at all, so it has to carry the
# same version 0 payload. The INSERT goes over HTTP: the TCP client parses Native input itself and
# does not handle binary-encoded types in it.
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&output_format_native_encode_types_in_binary_format=1" --data-binary "$query" > "$blob"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE dynamic_qd_rt"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&input_format_native_decode_types_in_binary_format=1&query=INSERT%20INTO%20dynamic_qd_rt%20FORMAT%20Native" --data-binary @"$blob"
echo "binary encoding: round trip $($CLICKHOUSE_CLIENT -q "SELECT medianDeterministicMerge(d.\`AggregateFunction(quantileDeterministic, UInt64, UInt64)\`) FROM dynamic_qd_rt")"

# The flattened Dynamic serialization announces its types through the same type list.
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&output_format_native_use_flattened_dynamic_and_json_serialization=1" --data-binary "$query" > "$blob"
echo "flattened: $(announced_version "$blob"), round trip $(roundtrip < "$blob")"

rm -f "$blob"

# A distributed query moves the whole Dynamic column over the Native wire before the subcolumn is
# extracted on the initiator; both sides spell the nested type the same way. Only the analyzer
# can read a Dynamic subcolumn out of a subquery result, and the subquery is what keeps the
# extraction on the initiator, so the query pins the analyzer instead of dropping the coverage.
$CLICKHOUSE_CLIENT -m -q "
SELECT 'remote: ' || toString(medianDeterministicMerge(d.\`AggregateFunction(quantileDeterministic, UInt64, UInt64)\`))
FROM (SELECT d FROM remote('127.0.0.2', currentDatabase(), dynamic_qd))
SETTINGS prefer_localhost_replica = 0, enable_analyzer = 1;
"

# The cost of all of the above, pinned so it does not change silently: a state inside a `Dynamic`
# value keeps state version 0, so the skip degree is not preserved across a `Dynamic` serialization
# boundary and a merge of two differently thinned states stays under-weighted, exactly as before the
# fix. The same 990000/10000 split stored in a plain `AggregateFunction` column merges to the correct
# 492708 (see 04653 and 04820).
$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS dynamic_qd_split;
CREATE TABLE dynamic_qd_split (d Dynamic) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dynamic_qd_split SELECT quantileDeterministicState(number, number) FROM numbers(990000);
INSERT INTO dynamic_qd_split SELECT quantileDeterministicState(number, number) FROM numbers(990000, 10000);
SELECT 'lopsided split via Dynamic: ' || toString(medianDeterministicMerge(d.\`AggregateFunction(quantileDeterministic, UInt64, UInt64)\`)) FROM dynamic_qd_split;
DROP TABLE dynamic_qd_split;
"

# A value in the shared variant embeds its type through the version-less binary encoding, followed
# by the serialized value - at rest and on the wire - so the state must be written there at version 0.
$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS dynamic_qd_shared;
CREATE TABLE dynamic_qd_shared (d Dynamic(max_types=0)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dynamic_qd_shared SELECT quantileDeterministicState(number, number) FROM numbers(1000000);
SELECT 'shared variant: ' || dynamicType(d) || ' ' || toString(isDynamicElementInSharedData(d)) FROM dynamic_qd_shared;
SELECT 'shared variant merge: ' || toString(medianDeterministicMerge(d.\`AggregateFunction(quantileDeterministic, UInt64, UInt64)\`)) FROM dynamic_qd_shared;
"

blob="${CLICKHOUSE_TMP}/04819_dynamic_shared.native"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}" --data-binary "SELECT d FROM ${CLICKHOUSE_DATABASE}.dynamic_qd_shared FORMAT Native" > "$blob"
echo "shared variant round trip: $(roundtrip < "$blob")"
rm -f "$blob"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE dynamic_qd;
DROP TABLE dynamic_qd_rt;
DROP TABLE dynamic_qd_shared;
"
