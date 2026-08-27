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
nested = pa.table({
    'n': pa.array([1, 2, 3], type=pa.int64()),
    'ln': pa.array([[None], [None, None], []], type=pa.list_(pa.null())),
})
with pa.OSFile('${FILE_PREFIX}.nested.arrow', 'wb') as sink:
    with pa.ipc.new_file(sink, nested.schema) as writer:
        writer.write_table(nested)
dict_null = pa.table({
    'n': pa.array([1, 2, 3], type=pa.int64()),
    'nd': pa.DictionaryArray.from_arrays(pa.array([0, None, 0], type=pa.int8()), pa.array([None], type=pa.null())),
})
with pa.OSFile('${FILE_PREFIX}.dict.arrow', 'wb') as sink:
    with pa.ipc.new_file(sink, dict_null.schema) as writer:
        writer.write_table(dict_null)
union_null = pa.table({
    'n': pa.array([1, 2, 3], type=pa.int64()),
    'u': pa.UnionArray.from_sparse(
        pa.array([0, 1, 0], type=pa.int8()),
        [pa.array([1, None, 3], type=pa.int32()),
         pa.array([None, [None, None], None], type=pa.list_(pa.null()))]),
})
with pa.OSFile('${FILE_PREFIX}.union.arrow', 'wb') as sink:
    with pa.ipc.new_file(sink, union_null.schema) as writer:
        writer.write_table(union_null)
"

echo 'file, explicit structure'
${CLICKHOUSE_LOCAL} -q "SELECT x, n FROM file('${FILE_PREFIX}.arrow', 'Arrow', 'x Nullable(UInt8), n UInt64') ORDER BY n"

echo 'stream, explicit structure'
${CLICKHOUSE_LOCAL} -q "SELECT x, n FROM file('${FILE_PREFIX}.arrows', 'ArrowStream', 'x Nullable(String), n UInt64') ORDER BY n"

echo 'schema inference maps the null-typed column to Nullable(Nothing)'
${CLICKHOUSE_LOCAL} -q "DESC file('${FILE_PREFIX}.arrow', 'Arrow')"

echo 'select * with the inferred schema'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${FILE_PREFIX}.arrow', 'Arrow') ORDER BY n"

echo 'a file whose only column is null-typed reads too'
${CLICKHOUSE_LOCAL} -q "SELECT *, count() OVER () FROM file('${FILE_PREFIX}.trivial.arrow', 'Arrow') LIMIT 1"

echo 'the null-typed column stays Nullable(Nothing) even with make_columns_nullable = 0'
${CLICKHOUSE_LOCAL} -q "DESC file('${FILE_PREFIX}.arrow', 'Arrow') SETTINGS schema_inference_make_columns_nullable = 0"

echo 'a nested null-typed field stays Nullable(Nothing) too'
${CLICKHOUSE_LOCAL} -q "DESC file('${FILE_PREFIX}.nested.arrow', 'Arrow') SETTINGS schema_inference_make_columns_nullable = 0"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${FILE_PREFIX}.nested.arrow', 'Arrow') ORDER BY n SETTINGS schema_inference_make_columns_nullable = 0"

echo 'a dictionary-encoded null-typed column materializes as all-null'
${CLICKHOUSE_LOCAL} -q "DESC file('${FILE_PREFIX}.dict.arrow', 'Arrow')"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${FILE_PREFIX}.dict.arrow', 'Arrow') ORDER BY n"

echo 'a union child carrying a nested null type reads too'
${CLICKHOUSE_LOCAL} -q "DESC file('${FILE_PREFIX}.union.arrow', 'Arrow')"
${CLICKHOUSE_LOCAL} -q "SELECT n, u FROM file('${FILE_PREFIX}.union.arrow', 'Arrow') ORDER BY n"

rm -f "${FILE_PREFIX}.arrow" "${FILE_PREFIX}.arrows" "${FILE_PREFIX}.trivial.arrow" "${FILE_PREFIX}.nested.arrow" "${FILE_PREFIX}.dict.arrow" "${FILE_PREFIX}.union.arrow"
