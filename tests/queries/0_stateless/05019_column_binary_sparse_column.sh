#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

# `ColumnBinaryOutputFormat::expectMaterializedColumns` returns false so that a top-level
# `ColumnConst` survives into `COL_IS_CONST`; that also skips the pipeline's materialization
# step, so a `ColumnSparse` read straight out of a `MergeTree` part reaches the writer. The
# fixed-width path there calls `IColumn::getRawData`, which `ColumnSparse` does not implement.

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_05019_src;
DROP TABLE IF EXISTS t_05019_dst;
CREATE TABLE t_05019_src (v UInt64, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1, min_bytes_for_wide_part = 0;
CREATE TABLE t_05019_dst (v UInt64, s String) ENGINE = Memory;
INSERT INTO t_05019_src SELECT if(number % 100 = 0, number, 0), if(number % 100 = 0, toString(number), '') FROM numbers(1000);
"

# Both columns must actually be stored sparsely, otherwise the test proves nothing.
${CLICKHOUSE_CLIENT} --query "
SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_05019_src' AND active ORDER BY column"

FILE="${CLICKHOUSE_TMP}/05019_sparse.bin"
rm -f "${FILE}"
${CLICKHOUSE_CLIENT} --query "SELECT v, s FROM t_05019_src INTO OUTFILE '${FILE}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_05019_dst FROM INFILE '${FILE}' FORMAT ColumnBinary"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v), countIf(s != '') FROM t_05019_dst"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE t_05019_src;
DROP TABLE t_05019_dst;
"
rm -f "${FILE}"
