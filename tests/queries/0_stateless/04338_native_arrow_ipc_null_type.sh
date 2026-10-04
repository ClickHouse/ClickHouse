#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Arrow format and pyarrow are not available in fasttest builds

# An Arrow column of the `null` type (every value is NULL) must be readable into a provided
# `Nullable` target and must behave as an unsupported type in schema inference (skippable with
# `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`), matching the
# Apache Arrow library based reader.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE_PREFIX="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

python3 -c "
import pyarrow as pa

table = pa.table({
    'x': pa.array([None, None, None], type=pa.null()),
    'n': pa.array([1, 2, 3], type=pa.int64()),
})
with pa.OSFile('${FILE_PREFIX}.arrow', 'wb') as sink:
    with pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
with pa.OSFile('${FILE_PREFIX}.arrows', 'wb') as sink:
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
"

echo 'file, explicit structure'
${CLICKHOUSE_LOCAL} -q "SELECT x, n FROM file('${FILE_PREFIX}.arrow', 'Arrow', 'x Nullable(UInt8), n UInt64') ORDER BY n"

echo 'stream, explicit structure'
${CLICKHOUSE_LOCAL} -q "SELECT x, n FROM file('${FILE_PREFIX}.arrows', 'ArrowStream', 'x Nullable(String), n UInt64') ORDER BY n"

echo 'schema inference skips the null column'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${FILE_PREFIX}.arrow', 'Arrow') ORDER BY n SETTINGS input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference = 1"

echo 'schema inference fails without the skip setting'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${FILE_PREFIX}.arrow', 'Arrow')" 2>&1 | grep -c 'UNKNOWN_TYPE'

rm -f "${FILE_PREFIX}.arrow" "${FILE_PREFIX}.arrows"
