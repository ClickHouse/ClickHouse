#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `vortex.timestamp` carries a unit and not a scale, so only the scales a unit spells exactly -
# 0, 3, 6 and 9 - come back from schema inference unchanged; the rest are written in the next wider
# unit. This pins what every scale does: the inferred type, and that the value itself survives both
# under inference and when the original type is named explicitly.

DATA_FILE=$CLICKHOUSE_TMP/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

for scale in 0 1 2 3 4 5 6 7 8 9; do
    echo "-- DateTime64($scale)"
    $CLICKHOUSE_LOCAL -m -q "
        SELECT toDateTime64('2026-09-18 12:34:56.123456789', $scale, 'UTC') AS t
        INTO OUTFILE '$DATA_FILE' TRUNCATE FORMAT Vortex;
    "
    $CLICKHOUSE_LOCAL -m -q "
        DESC file('$DATA_FILE', 'Vortex');
        SELECT t FROM file('$DATA_FILE', 'Vortex');
        SELECT t FROM file('$DATA_FILE', 'Vortex', 't DateTime64($scale, \'UTC\')');
    "
done

for scale in 0 1 2 3 4 5 6 7 8 9; do
    echo "-- Time64($scale)"
    $CLICKHOUSE_LOCAL -m -q "
        SELECT toTime64('12:34:56.123456789', $scale) AS t
        INTO OUTFILE '$DATA_FILE' TRUNCATE FORMAT Vortex;
    "
    $CLICKHOUSE_LOCAL -m -q "
        DESC file('$DATA_FILE', 'Vortex');
        SELECT t FROM file('$DATA_FILE', 'Vortex');
        SELECT t FROM file('$DATA_FILE', 'Vortex', 't Time64($scale)');
    "
done

rm -f "$DATA_FILE"
