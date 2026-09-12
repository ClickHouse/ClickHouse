#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc
rm -f "$FILE"

# A block can carry duplicate column names, but a NetCDF file with two variables of the same name
# would not be read back, so the writer has to reject it before writing anything.

echo "--- a result set with two columns of the same name cannot be written"
$CLICKHOUSE_LOCAL -q "SELECT x, x FROM (SELECT 1 AS x) FORMAT NetCDF" 2>&1 | grep -c "BAD_ARGUMENTS"

echo "--- the same through a file"
$CLICKHOUSE_LOCAL -q "SELECT x, x FROM (SELECT 1 AS x) INTO OUTFILE '$FILE' FORMAT NetCDF" 2>&1 | grep -c "BAD_ARGUMENTS"

echo "--- distinct names are written and read back"
rm -f "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x, 2 AS y INTO OUTFILE '$FILE' FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)"

rm -f "$FILE"
