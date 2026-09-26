#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `quantileDeterministicState` query result carries the explicitly versioned state type, so an
# export through a generic format writes a header and a payload that agree with each other and with
# a fresh table, whose unversioned column type gets the same version pinned at `CREATE` time. The
# round trip below would otherwise fail in two ways: with the default
# `input_format_with_types_use_header = 1` an unversioned header would be rejected against the
# pinned table type, and without the header check a version 0 payload would lose sync against the
# version 1 serialization of the target.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS qd_format_round_trip;

    -- The state type of a query result spells the version out.
    SELECT toTypeName(medianDeterministicState(number, number)) FROM numbers(1);

    CREATE TABLE qd_format_round_trip (part UInt8, state AggregateFunction(quantileDeterministic, UInt64, UInt64))
    ENGINE = MergeTree ORDER BY part;
"

# A very lopsided split: 990000 rows in one state, 10000 in the other. A split-independent merge
# must give the value a single state over all the rows gives (492708).
LOPSIDED="SELECT toUInt8(intDiv(number, 990000)) AS part, medianDeterministicState(number, number) AS state
          FROM numbers(1000000) GROUP BY part"

for format in RowBinaryWithNamesAndTypes TSVWithNamesAndTypes RowBinary
do
    # For the headerless RowBinary there are no types to check, so the round trip only works
    # because the query result and the pinned table serialize the state the same way.
    $CLICKHOUSE_CLIENT -q "$LOPSIDED FORMAT $format" \
        | $CLICKHOUSE_CLIENT -q "INSERT INTO qd_format_round_trip FORMAT $format"

    $CLICKHOUSE_CLIENT -q "
        SELECT medianDeterministicMerge(state) FROM qd_format_round_trip;
        TRUNCATE TABLE qd_format_round_trip;
    "
done

$CLICKHOUSE_CLIENT -q "DROP TABLE qd_format_round_trip"
