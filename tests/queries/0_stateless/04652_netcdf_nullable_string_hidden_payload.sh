#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc

# `nullIf` leaves the original value in the column under the NULLs, and that value can even be
# longer than every value that is not NULL. A NULL is written as an empty string regardless, and the
# hidden data must neither appear in the file nor stretch the dimension of the string.
$CLICKHOUSE_LOCAL -q "
    SELECT nullIf(repeat(toString(number), number), '4444') AS s,
           nullIf(toFixedString(toString(number) || 'x', 2), toFixedString('2x', 2)) AS f
    FROM numbers(5)
    INTO OUTFILE '$FILE' TRUNCATE FORMAT NetCDF"

$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF)"
$CLICKHOUSE_LOCAL -q "SELECT s, length(s), f, length(f) FROM file('$FILE', NetCDF)"

rm -f "$FILE"
