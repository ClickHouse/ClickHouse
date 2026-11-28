#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A temporal bound is pushed into the scan only when the scan compares exactly what ClickHouse
# compares: not a timestamp cut down to the date of a `Date32` column, and not a floating-point
# value, which ClickHouse compares with a `DateTime64` column as `Float64`. Each query must give the
# same result with the filter pushdown on and off.

DATA_FILE=$CLICKHOUSE_TMP/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

$CLICKHOUSE_LOCAL -q "
    SELECT
        toDate32('2020-01-01') AS d,
        toDateTime64('2020-01-01 00:00:00.000000001', 9, 'UTC') AS dt
    INTO OUTFILE '$DATA_FILE' TRUNCATE FORMAT Vortex;
"

STRUCTURE="d Date32, dt DateTime64(9, 'UTC')"

for condition in \
    "d < toDateTime('2020-01-01 12:00:00', 'UTC')" \
    "d NOT IN (SELECT toDateTime('2020-01-01 12:00:00', 'UTC'))" \
    "dt = toFloat64(1577836800)" \
    "dt IN (SELECT toFloat64(1577836800))"
do
    echo "-- $condition"
    for push_down in 1 0; do
        $CLICKHOUSE_LOCAL -q "
            SELECT count() FROM file('$DATA_FILE', 'Vortex', \"$STRUCTURE\") WHERE $condition
            SETTINGS input_format_vortex_filter_push_down = $push_down, session_timezone = 'UTC';
        "
    done
done

rm -f "$DATA_FILE"
