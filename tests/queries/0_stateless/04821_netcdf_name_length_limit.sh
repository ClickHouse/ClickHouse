#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# The reference library limits a name to `NC_MAX_NAME` = 256 bytes, and its readers allocate only
# that much for a name, so the writer rejects a longer name instead of producing a file that common
# readers cannot safely open.

echo "--- a name of exactly 256 bytes is written and read back"
NAME=$(printf 'a%.0s' {1..256})
$CLICKHOUSE_LOCAL -q "SELECT 1 AS \`$NAME\` INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF)" | awk '{print length($1), $2}'
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)"
rm -f "$FILE"

echo "--- a name of 257 bytes cannot be written"
$CLICKHOUSE_LOCAL -q "SELECT 1 AS \`${NAME}b\` FORMAT NetCDF" 2>&1 | grep -c "BAD_ARGUMENTS"

echo "--- a String column whose generated length dimension crosses the bound cannot either"
# The name of the column itself fits, but the name of its string-length dimension is the name of
# the column plus the '_strlen' suffix, which is over the bound.
$CLICKHOUSE_LOCAL -q "SELECT 's' AS \`$NAME\` FORMAT NetCDF" 2>&1 | grep -c "BAD_ARGUMENTS"

echo "--- and a String column whose length dimension is exactly at the bound is written"
SHORTER=$(printf 'a%.0s' {1..249})
$CLICKHOUSE_LOCAL -q "SELECT 's' AS \`$SHORTER\` INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF)" | awk '{print length($1), $2}'
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', NetCDF)"
rm -f "$FILE"
