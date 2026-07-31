#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# A string shorter than the dimension of its variable is padded with zero bytes, so a value that
# itself ends in a zero byte cannot be stored: it would be read back without its trailing zero
# bytes. Writing such a value must throw instead of corrupting the value.
$CLICKHOUSE_LOCAL -q "SELECT reinterpretAsString(unhex('4100')) AS s
    INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF" 2>&1 | grep -c 'BAD_ARGUMENTS'

$CLICKHOUSE_LOCAL -q "SELECT toFixedString('A', 2) AS f
    INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF" 2>&1 | grep -c 'BAD_ARGUMENTS'

# A zero byte anywhere else survives the round trip.
$CLICKHOUSE_LOCAL -q "SELECT reinterpretAsString(unhex('00410042')) AS s
    INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT hex(s) FROM file('$FILE', NetCDF)"

# A value hidden under a NULL is not written, so it is allowed to end in a zero byte.
$CLICKHOUSE_LOCAL -q "SELECT nullIf(reinterpretAsString(unhex('4100')), reinterpretAsString(unhex('4100'))) AS s
    INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"
$CLICKHOUSE_LOCAL -q "SELECT hex(s), length(s) FROM file('$FILE', NetCDF)"

rm -f "$FILE"
