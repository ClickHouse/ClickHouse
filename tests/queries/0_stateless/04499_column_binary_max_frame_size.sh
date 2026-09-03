#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

FRAME_FILE="${CLICKHOUSE_TMP}/04499_column_binary_frame.bin"

${CLICKHOUSE_CLIENT} --query "SELECT repeat('a', 1000) AS s FROM numbers(10) FORMAT ColumnBinary" > "${FRAME_FILE}"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_04499;
CREATE TABLE t_04499 (s String) ENGINE = Memory;
"

# A frame comfortably under the limit is accepted.
${CLICKHOUSE_CLIENT} --column_binary_max_frame_size=1000000 --query "INSERT INTO t_04499 FROM INFILE '${FRAME_FILE}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(length(s)) FROM t_04499"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE t_04499"

# The same frame is rejected before allocation once the cap is set below its actual size.
${CLICKHOUSE_CLIENT} --column_binary_max_frame_size=100 --query "INSERT INTO t_04499 FROM INFILE '${FRAME_FILE}' FORMAT ColumnBinary" 2>&1 \
  | grep -o "column_binary_max_frame_size limit"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04499"
rm -f "${FRAME_FILE}"
