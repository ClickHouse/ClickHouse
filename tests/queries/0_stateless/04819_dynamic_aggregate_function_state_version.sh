#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A fresh aggregate function state spells its version out in its type, and `Dynamic` transports the
# spelled type faithfully on every medium: its type lists carry the full type name, and the binary
# type encoding has an explicit version field (the values of the shared variant embed their type
# that way even at rest). No medium under `Dynamic` re-derives the version from the negotiated
# revision, so the announcement and the payload always agree, for every peer - an old peer is told
# the explicitly versioned type instead of getting a silently downgraded payload. A state whose type
# does not spell a version (data written before the version existed, or an explicit unversioned
# `CAST`) keeps state version 0 and the exact bytes unversioned data always had.
# https://github.com/ClickHouse/ClickHouse/pull/112052

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS dynamic_qd;
DROP TABLE IF EXISTS dynamic_qd_rt;
CREATE TABLE dynamic_qd (d Dynamic) ENGINE = MergeTree ORDER BY tuple();
-- Enough rows for the reservoir to be thinned out: a state version 1 payload has a non-zero
-- skip degree to write, which is exactly where the versions differ.
INSERT INTO dynamic_qd SELECT quantileDeterministicState(number, number) FROM numbers(1000000);
INSERT INTO dynamic_qd VALUES (42::UInt8);

-- The nested type spells the state version out.
SELECT DISTINCT dynamicType(d) FROM dynamic_qd ORDER BY 1;

-- The state is readable in place through the versioned subcolumn name.
SELECT medianDeterministicMerge(d.\`AggregateFunction(1, quantileDeterministic, UInt64, UInt64)\`) FROM dynamic_qd;

CREATE TABLE dynamic_qd_rt (d Dynamic) ENGINE = Memory;
"

query="SELECT d FROM ${CLICKHOUSE_DATABASE}.dynamic_qd FORMAT Native"

# When a query fails after streaming has started, the server breaks the HTTP protocol on
# purpose - the terminating empty chunk is never sent (a fixed-length response is cut short
# the same way) - so `curl` exits with an error instead of passing the poisoned stream off
# as a result; `--fail` covers a failure that comes before the first byte. Without the
# check, a transient error (e.g. a parallel-replicas teardown race, see issue #116341)
# would feed exception text into the `INSERT ... FORMAT Native` round trips below and fail
# them with a misleading `Unknown type code`. A recognized broken stream (`curl` exit code
# 18) is retried: the transient is not what this test covers. Any other failure is
# deterministic and aborts the test at once - the harness runs the script without `errexit`,
# so a `return` would let it keep going on a partial blob.
fetch_blob()
{
    local url="$1" fetch_query="$2" out="$3" error status=0
    for _ in {1..10}
    do
        error=$($CLICKHOUSE_CURL --fail -sS "$url" --data-binary "$fetch_query" 2>&1 > "$out") && return 0
        status=$?
        if [ "$status" -ne 18 ]
        then
            break
        fi
    done
    echo "failed to fetch a Native blob (curl exit code $status): $error"
    exit 1
}

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
    $CLICKHOUSE_CLIENT -q "SELECT medianDeterministicMerge(d.\`AggregateFunction(1, quantileDeterministic, UInt64, UInt64)\`) FROM dynamic_qd_rt"
}

blob="${CLICKHOUSE_TMP}/04819_dynamic.native"

# The nested state does not follow the negotiated revision: a peer that predates the state version,
# no negotiated revision at all, and a current peer are all told the same explicitly versioned
# nested type.
fetch_blob "${CLICKHOUSE_URL}&client_protocol_version=54337" "$query" "$blob"
echo "old peer: $(announced_version "$blob")"
fetch_blob "${CLICKHOUSE_URL}&client_protocol_version=54491" "$query" "$blob"
echo "current peer: $(announced_version "$blob")"

# A versioned announcement carries a version 1 payload.
fetch_blob "${CLICKHOUSE_URL}" "$query" "$blob"
echo "no revision: $(announced_version "$blob"), round trip $(roundtrip < "$blob")"

# The binary type encoding carries the version in its explicit version field, so the payload stays
# version 1 there too. The INSERT goes over HTTP: the TCP client parses Native input itself and
# does not handle binary-encoded types in it.
fetch_blob "${CLICKHOUSE_URL}&output_format_native_encode_types_in_binary_format=1" "$query" "$blob"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE dynamic_qd_rt"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&input_format_native_decode_types_in_binary_format=1&query=INSERT%20INTO%20dynamic_qd_rt%20FORMAT%20Native" --data-binary @"$blob"
echo "binary encoding: round trip $($CLICKHOUSE_CLIENT -q "SELECT medianDeterministicMerge(d.\`AggregateFunction(1, quantileDeterministic, UInt64, UInt64)\`) FROM dynamic_qd_rt")"

# The flattened Dynamic serialization announces its types through the same type list.
fetch_blob "${CLICKHOUSE_URL}&output_format_native_use_flattened_dynamic_and_json_serialization=1" "$query" "$blob"
echo "flattened: $(announced_version "$blob"), round trip $(roundtrip < "$blob")"

rm -f "$blob"

# A distributed query moves the whole Dynamic column over the Native wire before the subcolumn is
# extracted on the initiator; both sides spell the nested type the same way. Only the analyzer
# can read a Dynamic subcolumn out of a subquery result, and the subquery is what keeps the
# extraction on the initiator, so the query pins the analyzer instead of dropping the coverage.
$CLICKHOUSE_CLIENT -m -q "
SELECT 'remote: ' || toString(medianDeterministicMerge(d.\`AggregateFunction(1, quantileDeterministic, UInt64, UInt64)\`))
FROM (SELECT d FROM remote('127.0.0.2', currentDatabase(), dynamic_qd))
SETTINGS prefer_localhost_replica = 0, enable_analyzer = 1;
"

# Because the version travels with the type, the skip degree survives the `Dynamic` serialization
# boundary and a merge of two differently thinned states is split-independent, the same as in a
# plain `AggregateFunction` column (see 04653 and 04820).
$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS dynamic_qd_split;
CREATE TABLE dynamic_qd_split (d Dynamic) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dynamic_qd_split SELECT quantileDeterministicState(number, number) FROM numbers(990000);
INSERT INTO dynamic_qd_split SELECT quantileDeterministicState(number, number) FROM numbers(990000, 10000);
SELECT 'lopsided split via Dynamic: ' || toString(medianDeterministicMerge(d.\`AggregateFunction(1, quantileDeterministic, UInt64, UInt64)\`)) FROM dynamic_qd_split;
DROP TABLE dynamic_qd_split;
"

# An explicitly unversioned spelling keeps state version 0 and the old byte layout - that is what
# keeps data written before the version existed readable. The cost is the pre-fix behavior, pinned
# here so it does not change silently: the skip degree is dropped, so the same lopsided merge stays
# under-weighted.
$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS dynamic_qd_unversioned;
CREATE TABLE dynamic_qd_unversioned (d Dynamic) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dynamic_qd_unversioned SELECT CAST(quantileDeterministicState(number, number), 'AggregateFunction(quantileDeterministic, UInt64, UInt64)') FROM numbers(990000);
INSERT INTO dynamic_qd_unversioned SELECT CAST(quantileDeterministicState(number, number), 'AggregateFunction(quantileDeterministic, UInt64, UInt64)') FROM numbers(990000, 10000);
SELECT 'unversioned spelling: ' || dynamicType(d) FROM dynamic_qd_unversioned LIMIT 1;
SELECT 'unversioned lopsided merge: ' || toString(medianDeterministicMerge(d.\`AggregateFunction(quantileDeterministic, UInt64, UInt64)\`)) FROM dynamic_qd_unversioned;
DROP TABLE dynamic_qd_unversioned;
"

# A value in the shared variant embeds its type through the binary encoding, followed by the
# serialized value - at rest and on the wire - and the encoding's version field keeps the
# announcement and the payload in agreement there as well.
$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS dynamic_qd_shared;
CREATE TABLE dynamic_qd_shared (d Dynamic(max_types=0)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dynamic_qd_shared SELECT quantileDeterministicState(number, number) FROM numbers(1000000);
SELECT 'shared variant: ' || dynamicType(d) || ' ' || toString(isDynamicElementInSharedData(d)) FROM dynamic_qd_shared;
SELECT 'shared variant merge: ' || toString(medianDeterministicMerge(d.\`AggregateFunction(1, quantileDeterministic, UInt64, UInt64)\`)) FROM dynamic_qd_shared;
"

blob="${CLICKHOUSE_TMP}/04819_dynamic_shared.native"
fetch_blob "${CLICKHOUSE_URL}" "SELECT d FROM ${CLICKHOUSE_DATABASE}.dynamic_qd_shared FORMAT Native" "$blob"
echo "shared variant round trip: $(roundtrip < "$blob")"
rm -f "$blob"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE dynamic_qd;
DROP TABLE dynamic_qd_rt;
DROP TABLE dynamic_qd_shared;
"
