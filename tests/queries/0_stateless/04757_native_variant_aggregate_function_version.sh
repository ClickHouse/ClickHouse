#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: a `Replicated` database re-parses the column type from the formatted
# `CREATE`, and version 0 is not printed in the type name, so the version pin of the legacy table
# below does not survive the round trip through the DDL log.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A versioned aggregate function state inside a `Variant` has to follow the version negotiated for the
# `Native` stream, exactly like a bare state column: the writer must announce and write the version
# derived from the peer's revision (not whatever version sits on its local type), and the reader must
# pin the version of an unversioned announcement to the sender's revision. Missing the `Variant`
# carrier in that recursion broke both directions for
# `Variant(AggregateFunction(quantileDeterministic, UInt64, UInt64), UInt8)`.
# https://github.com/ClickHouse/ClickHouse/pull/112052

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS variant_qd;
CREATE TABLE variant_qd (v Variant(AggregateFunction(quantileDeterministic, UInt64, UInt64), UInt8))
ENGINE = MergeTree ORDER BY tuple();
-- Enough rows for the reservoir to be thinned out, so that version 1 has a non-zero skip degree to write.
-- Inserting into a Variant needs the exact alternative type: a fresh CREATE pins the current state
-- version explicitly, while a freshly built state is unversioned, so spell the version out.
INSERT INTO variant_qd SELECT CAST(quantileDeterministicState(number, number), 'AggregateFunction(1, quantileDeterministic, UInt64, UInt64)') FROM numbers(1000000);
INSERT INTO variant_qd VALUES (42);
"

query="SELECT v FROM ${CLICKHOUSE_DATABASE}.variant_qd FORMAT Native"

announced_version()
{
    local blob="$1"
    if ! LC_ALL=C grep -aqF 'Variant(' "$blob"; then
        echo "the Variant wrapper was lost"
    elif LC_ALL=C grep -aqF 'AggregateFunction(1,' "$blob"; then
        echo "version 1"
    else
        echo "no version"
    fi
}

# A peer that predates aggregate function state versioning gets version 0 states, so the type it is
# told about must not mention a version either.
old_blob="${CLICKHOUSE_TMP}/04757_old.native"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&client_protocol_version=54337" --data-binary "$query" > "$old_blob"
echo "old peer: $(announced_version "$old_blob")"

# `FORMAT Native` with no negotiated revision at all has no peer to derive a version for: the stream
# is self-describing, so the version pinned on the type survives into it and is announced as such.
default_blob="${CLICKHOUSE_TMP}/04757_default.native"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}" --data-binary "$query" > "$default_blob"
echo "no revision: $(announced_version "$default_blob")"

# A current peer gets version 1 states inside the Variant and is told so.
new_blob="${CLICKHOUSE_TMP}/04757_new.native"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&client_protocol_version=54491" --data-binary "$query" > "$new_blob"
echo "current peer: $(announced_version "$new_blob")"

rm -f "$old_blob" "$default_blob" "$new_blob"

# The other direction: a legacy table whose state version inside the Variant is pinned to 0 (what a
# table created before the state had a version looks like once attached on a current server). The wire
# version must come from the negotiated revision on both ends, so the state has to survive a round trip
# through `Native` transport unchanged.
$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS legacy_variant_qd;
CREATE TABLE legacy_variant_qd (v Variant(AggregateFunction(0, quantileDeterministic, UInt64, UInt64), UInt8))
ENGINE = MergeTree ORDER BY tuple();
-- Inserting into a Variant needs the exact alternative type, so pin the freshly built state to
-- version 0 explicitly.
INSERT INTO legacy_variant_qd SELECT CAST(quantileDeterministicState(number, number), 'AggregateFunction(0, quantileDeterministic, UInt64, UInt64)') FROM numbers(1000000);
INSERT INTO legacy_variant_qd VALUES (42);

-- The version is pinned to 0, and version 0 is not printed in the type name.
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'legacy_variant_qd';

-- Reading the state locally, and through a distributed query that sends the raw Variant column over
-- the wire, must both give the same values.
SELECT
    medianDeterministicMerge(v.\`AggregateFunction(quantileDeterministic, UInt64, UInt64)\`),
    sum(v.UInt8)
FROM legacy_variant_qd;

-- The round trip between two current peers re-versions the state to the negotiated revision on the
-- wire, but the default (unversioned) spelling of the type stays version 0, so the subcolumn keeps
-- its unversioned name on the initiator and on the shard alike, whichever side extracts it.
SELECT
    medianDeterministicMerge(v.\`AggregateFunction(quantileDeterministic, UInt64, UInt64)\`),
    sum(v.UInt8)
FROM (SELECT v FROM remote('127.0.0.2', currentDatabase(), legacy_variant_qd))
SETTINGS prefer_localhost_replica = 0;

DROP TABLE legacy_variant_qd;
DROP TABLE variant_qd;
"
