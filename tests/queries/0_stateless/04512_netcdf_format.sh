#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The files in data_netcdf were written by the netCDF C library through its Python bindings.
DATA=$CUR_DIR/data_netcdf

for version in cdf1 cdf2 cdf5
do
    echo "--- $version"
    $CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA/example_$version.nc')"
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/example_$version.nc') ORDER BY time, lat, lon"
    $CLICKHOUSE_LOCAL -q "SELECT count(), sum(temp), uniqExact(station), min(height) FROM file('$DATA/example_$version.nc')"
done

echo "--- the format is detected from the extension"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA/example_cdf1.nc')"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA/example_cdf1.nc', NetCDF)"

echo "--- only the requested variables are read"
$CLICKHOUSE_LOCAL -q "SELECT DISTINCT station FROM file('$DATA/example_cdf1.nc') ORDER BY station"
$CLICKHOUSE_LOCAL -q "SELECT DISTINCT lon FROM file('$DATA/example_cdf1.nc') ORDER BY lon"

echo "--- the number of rows is taken from the header"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA/example_cdf1.nc') SETTINGS optimize_count_from_files = 1"

echo "--- the types can be overridden"
$CLICKHOUSE_LOCAL -q "SELECT temp, toTypeName(temp) FROM file('$DATA/example_cdf1.nc', NetCDF, 'temp Float64') LIMIT 1"

echo "--- a fill value is read as NULL only when it is asked for"
$CLICKHOUSE_LOCAL -q "SELECT countIf(pressure = -9999) FROM file('$DATA/example_cdf1.nc')"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA/example_cdf1.nc') SETTINGS input_format_netcdf_fill_value_as_null = 1"
$CLICKHOUSE_LOCAL -q "SELECT countIf(pressure IS NULL) FROM file('$DATA/example_cdf1.nc') SETTINGS input_format_netcdf_fill_value_as_null = 1"

echo "--- the indexes along the dimensions can be added as columns"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/grid.nc') ORDER BY value"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/grid.nc') ORDER BY value SETTINGS input_format_netcdf_add_dimension_columns = 1"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA/example_cdf1.nc') SETTINGS input_format_netcdf_add_dimension_columns = 1"

echo "--- an input that cannot be seeked is read as well"
$CLICKHOUSE_LOCAL -q "SELECT sum(temp) FROM table" --input-format NetCDF --structure "temp Float32" < "$DATA/example_cdf1.nc"
cat "$DATA/example_cdf1.nc" | $CLICKHOUSE_LOCAL -q "SELECT sum(temp) FROM table" --input-format NetCDF --structure "temp Float32"

echo "--- errors"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/netcdf4.nc', NetCDF)" 2>&1 | grep -c "NetCDF-4"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/example_cdf1.nc', NetCDF, 'no_such_variable Int32')" 2>&1 | grep -c "THERE_IS_NO_COLUMN"
echo -n "not a netcdf file at all" | $CLICKHOUSE_LOCAL -q "SELECT * FROM table" --input-format NetCDF --structure "x Int32" 2>&1 | grep -c "INCORRECT_DATA"
