#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# Nullable(Tuple(...)) is the only Nullable(Array/Tuple/Variant) signature ClickHouse can
# actually construct (DataTypeArray/DataTypeVariant::canBeInsideNullable() are both false),
# reachable behind enable_nullable_tuple_type. Round-trip it through FORMAT ColumnBinary to
# exercise buildColDescriptor/writeColData's top-level null map on a COL_COMPLEX column.
FRAME_FILE="${CLICKHOUSE_TMP}/04505_column_binary_frame.bin"

${CLICKHOUSE_CLIENT} --enable_nullable_tuple_type=1 --query "
SELECT if(number % 3 = 0, NULL, tuple(number, toString(number)))::Nullable(Tuple(UInt64, String)) AS t
FROM numbers(6)
FORMAT ColumnBinary" > "${FRAME_FILE}"

${CLICKHOUSE_CLIENT} --enable_nullable_tuple_type=1 --multiquery --query "
DROP TABLE IF EXISTS t_04505;
CREATE TABLE t_04505 (t Nullable(Tuple(UInt64, String))) ENGINE = Memory;
"

${CLICKHOUSE_CLIENT} --enable_nullable_tuple_type=1 --query "INSERT INTO t_04505 FROM INFILE '${FRAME_FILE}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "SELECT t FROM t_04505 ORDER BY t"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04505"
rm -f "${FRAME_FILE}"
