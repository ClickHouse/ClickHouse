#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# The CF conventions name only the units of the scales 0, 3, 6 and 9, so a `DateTime64` of another
# scale is written in the next finer named unit, with the values multiplied accordingly. The
# `units` attribute is stored as text in the header, so it is visible to grep.

echo '--- scale 1 becomes milliseconds'
$CLICKHOUSE_LOCAL -q "SELECT toDateTime64('2001-02-03 04:05:06.7', 1, 'UTC') AS t
    INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
grep -ac 'milliseconds since 1970-01-01' "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT t, fromUnixTimestamp64Milli(t, 'UTC') FROM file('$FILE', NetCDF)"

echo '--- scale 4 becomes microseconds'
$CLICKHOUSE_LOCAL -q "SELECT toDateTime64('2001-02-03 04:05:06.7891', 4, 'UTC') AS t
    INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
grep -ac 'microseconds since 1970-01-01' "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT t, fromUnixTimestamp64Micro(t, 'UTC') FROM file('$FILE', NetCDF)"

echo '--- scale 7 becomes nanoseconds, before the epoch as well'
$CLICKHOUSE_LOCAL -q "SELECT toDateTime64('1955-02-03 04:05:06.7891234', 7, 'UTC') AS t
    INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
grep -ac 'nanoseconds since 1970-01-01' "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT t, fromUnixTimestamp64Nano(t, 'UTC') FROM file('$FILE', NetCDF)"

echo '--- a named scale is written as it is'
$CLICKHOUSE_LOCAL -q "SELECT toDateTime64('2001-02-03 04:05:06.789123', 6, 'UTC') AS t
    INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
grep -ac 'microseconds since 1970-01-01' "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT t FROM file('$FILE', NetCDF)"

echo '--- a NULL is not rescaled and the fill value avoids the rescaled data'
$CLICKHOUSE_LOCAL -q "SELECT if(number = 1, NULL, toDateTime64('2001-02-03 04:05:06.7', 1, 'UTC'))::Nullable(DateTime64(1, 'UTC')) AS t
    FROM numbers(3) INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT t FROM file('$FILE', NetCDF)
    SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo '--- a value that does not fit after the rescale throws'
$CLICKHOUSE_LOCAL -q "SELECT toDateTime64('2299-12-31 23:59:59.99999999', 8, 'UTC') AS t
    INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF" 2>&1 | grep -c 'BAD_ARGUMENTS'

rm -f "$FILE"
