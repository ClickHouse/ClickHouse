#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# `data_binary/column_binary_const.bin` is a checked-in frame whose second and third column
# descriptors carry the `COL_IS_CONST` flag, each storing a single value for all 5 rows. No
# `SELECT` of a literal produces such a frame - constants are materialized before they reach
# the output format - so a const frame only ever arrives from a foreign producer, such as a
# WASM guest writing the wire format directly.
FRAME="$CUR_DIR/data_binary/column_binary_const.bin"
STRUCTURE="n UInt64, c UInt64, s String"

# `file()` is confined to `user_files`, so read the fixture through `clickhouse-local`.
CLICKHOUSE_LOCAL="${CLICKHOUSE_LOCAL} --allow_experimental_column_binary_format 1"

# The const value must be replicated across every row.
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${FRAME}', ColumnBinary, '${STRUCTURE}') ORDER BY n"

# ... and it must be replicated by materializing it, not by handing the pipeline a
# `ColumnConst`: the header carries the plain column the declared type creates, so a wrapper
# here makes the chunk disagree with its own header.
${CLICKHOUSE_LOCAL} --query "
    SELECT DISTINCT dumpColumnStructure(c), dumpColumnStructure(s)
    FROM file('${FRAME}', ColumnBinary, '${STRUCTURE}')"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t_05059;
CREATE TABLE t_05059 (n UInt64, c UInt64, s String) ENGINE = MergeTree ORDER BY n;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_05059 FROM INFILE '${FRAME}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(c), uniqExact(s) FROM t_05059"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05059"
