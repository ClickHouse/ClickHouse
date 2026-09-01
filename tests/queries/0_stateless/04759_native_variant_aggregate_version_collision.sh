#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: a `Replicated` database re-parses the column type from the formatted
# `CREATE`, and version 0 is not printed in the type name, so the version-0 pin does not survive the
# DDL log: the re-parsed alternative spells the default version and the `Variant` collapses.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `Variant` may hold two aggregate function states that differ only in the pinned state version,
# e.g. `AggregateFunction(0, quantileDeterministic, ...)` and
# `AggregateFunction(1, quantileDeterministic, ...)`. Re-versioning such a column for the `Native`
# wire gives both alternatives the negotiated version, which collapses them into the same type, so
# the column cannot be represented at any negotiated revision. The writer must fail rather than
# keep the local versions and announce types the payload does not match.
# https://github.com/ClickHouse/ClickHouse/pull/112052

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS variant_version_collision;
-- The two alternatives differ only in the state version, which the similar-types validation
-- rightly considers suspicious.
SET allow_suspicious_variant_types = 1;
CREATE TABLE variant_version_collision
(v Variant(AggregateFunction(0, quantileDeterministic, UInt64, UInt64), AggregateFunction(1, quantileDeterministic, UInt64, UInt64)))
ENGINE = MergeTree ORDER BY tuple();

-- Version 0 is not printed in a type name, so the two alternatives spell differently.
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'variant_version_collision';

-- Inserting into a Variant needs the exact alternative type; the INSERT ... SELECT runs entirely
-- on the server, so nothing crosses the Native wire yet.
INSERT INTO variant_version_collision SELECT CAST(quantileDeterministicState(number, number), 'AggregateFunction(0, quantileDeterministic, UInt64, UInt64)') FROM numbers(100);

-- No wire involved: the states are readable in place (the version-0 alternative's subcolumn is
-- named without the version).
SELECT medianDeterministicMerge(v.\`AggregateFunction(quantileDeterministic, UInt64, UInt64)\`) FROM variant_version_collision;
"

# Selecting the column over the native protocol re-versions both alternatives to the negotiated
# version, which collapses them - a hard error, not a silent no-op that would announce versions the
# payload is not written with.
$CLICKHOUSE_CLIENT -q "SELECT v FROM variant_version_collision FORMAT Null" |& grep -oF 'BAD_ARGUMENTS' | head -1

# The downgrade direction: a peer that predates state versioning gets version 0 for both
# alternatives, which is the same collapse.
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&client_protocol_version=54337" --data-binary "SELECT v FROM ${CLICKHOUSE_DATABASE}.variant_version_collision FORMAT Native" |& grep -oF 'BAD_ARGUMENTS' | head -1

$CLICKHOUSE_CLIENT -q "DROP TABLE variant_version_collision"
