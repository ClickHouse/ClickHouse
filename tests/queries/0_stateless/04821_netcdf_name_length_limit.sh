#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# The reader rejects a name longer than 65536 bytes as a sign of a corrupted file, so the writer
# has to reject such a name as well, or it would produce a file that cannot be read back.

echo "--- a name of exactly 65536 bytes is written and read back"
NAME=$(printf 'a%.0s' {1..65536})
$CLICKHOUSE_LOCAL -q "SELECT 1 AS \`$NAME\` INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF)" | awk '{print length($1), $2}'
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)"
rm -f "$FILE"

echo "--- a name of 65537 bytes cannot be written"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS \`${NAME}b\` FORMAT NetCDF" 2>&1 | grep -c "BAD_ARGUMENTS"

echo "--- a String column whose generated length dimension crosses the bound cannot either"
# The name of the column itself fits, but the name of its string-length dimension is the name of
# the column plus the '_strlen' suffix, which is over the bound.
$CLICKHOUSE_LOCAL -q "SELECT 's' AS \`$NAME\` FORMAT NetCDF" 2>&1 | grep -c "BAD_ARGUMENTS"

echo "--- and a String column whose length dimension is exactly at the bound is written"
SHORTER=$(printf 'a%.0s' {1..65529})
$CLICKHOUSE_LOCAL -q "SELECT 's' AS \`$SHORTER\` INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF)" | awk '{print length($1), $2}'
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)"
rm -f "$FILE"
