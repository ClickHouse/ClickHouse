#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Arrow format and pyarrow are not available in fasttest builds

# An Arrow column of the `null` type (every value is NULL) must be readable into a provided
# `Nullable` target, and schema inference must map it to `Nullable(Nothing)` (an all-null column)
# so a file with null-typed columns — even one where EVERY column is null-typed — reads without any
# settings, matching the Apache Arrow library based reader.

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
trivial = pa.table({'only': pa.array([None] * 4, type=pa.null())})
with pa.OSFile('${FILE_PREFIX}.trivial.arrow', 'wb') as sink:
    with pa.ipc.new_file(sink, trivial.schema) as writer:
        writer.write_table(trivial)
"

echo 'file, explicit structure'
${CLICKHOUSE_LOCAL} -q "SELECT x, n FROM file('${FILE_PREFIX}.arrow', 'Arrow', 'x Nullable(UInt8), n UInt64') ORDER BY n"

echo 'stream, explicit structure'
${CLICKHOUSE_LOCAL} -q "SELECT x, n FROM file('${FILE_PREFIX}.arrows', 'ArrowStream', 'x Nullable(String), n UInt64') ORDER BY n"

echo 'schema inference maps the null column to Nullable(Nothing)'
${CLICKHOUSE_LOCAL} -q "DESC file('${FILE_PREFIX}.arrow', 'Arrow')"

echo 'select * with the inferred schema'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${FILE_PREFIX}.arrow', 'Arrow') ORDER BY n"

echo 'a file whose only column is null-typed reads too'
${CLICKHOUSE_LOCAL} -q "SELECT *, count() OVER () FROM file('${FILE_PREFIX}.trivial.arrow', 'Arrow') LIMIT 1"

rm -f "${FILE_PREFIX}.arrow" "${FILE_PREFIX}.arrows" "${FILE_PREFIX}.trivial.arrow"
