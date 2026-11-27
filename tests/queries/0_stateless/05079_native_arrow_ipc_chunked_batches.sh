#!/usr/bin/env bash
# Tags: no-fasttest
# This test requires PyArrow to write Arrow batches with explicit boundaries and dictionary updates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PYEOF'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]
list_type = pa.list_(pa.null(), 2)
struct_type = pa.struct([pa.field("a", pa.null()), pa.field("b", list_type, nullable=False)])
struct_value = {"a": None, "b": [None, None]}
root_fields = [
    pa.field("n", pa.null()),
    pa.field("s", struct_type, nullable=False),
    pa.field("f", list_type, nullable=False),
    pa.field("e", pa.struct([]), nullable=False),
]


def write(name, schema, batches, deltas=False, formats=("Arrow", "ArrowStream")):
    options = ipc.IpcWriteOptions(emit_dictionary_deltas=deltas)
    for fmt in formats:
        new_writer = ipc.new_file if fmt == "Arrow" else ipc.new_stream
        with new_writer(f"{out}/{name}.{fmt}", schema, options=options) as writer:
            for batch in batches:
                writer.write_batch(batch)


def roots(rows):
    return [
        pa.nulls(rows),
        pa.array([struct_value] * rows, type=struct_type),
        pa.array([[None, None]] * rows, type=list_type),
        pa.array([{}] * rows, type=pa.struct([])),
    ]


# Ordinary batches keep their boundaries. Empty batches can precede, separate, or follow them.
schema = pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    *root_fields,
    pa.field("v", pa.int32()),
    pa.field("t", pa.struct([pa.field("a", pa.int32(), nullable=False)]), nullable=False),
])
batches = []
offset = 0
for rows in (0, 5, 0, 4, 0):
    ids = list(range(offset, offset + rows))
    arrays = [
        pa.array(ids, type=pa.int32()),
        *roots(rows),
        pa.array([value if value % 2 else None for value in ids], type=pa.int32()),
        pa.array([{"a": value + 10} for value in ids], type=schema.field("t").type),
    ]
    batches.append(pa.record_batch(arrays, schema=schema))
    offset += rows
write("mixed", schema, batches)
write("empty", schema, [batches[0]])

# A single-row batch uses whole-column conversion; the larger bufferless batch requires slicing.
root_schema = pa.schema(root_fields)
write("bufferless", root_schema, [pa.record_batch(roots(rows), schema=root_schema) for rows in (1, 7, 0)])
nullable_struct_schema = pa.schema([pa.field("s", struct_type)])
write("nullable_struct", nullable_struct_schema, [pa.record_batch(
    [pa.array([struct_value] * 5, type=struct_type)], schema=nullable_struct_schema)])

# Repeated complex dictionary values retain their logical indices while sharing one physical value.
repeated_columns = [
    pa.DictionaryArray.from_arrays(pa.array([6, 2, 0, 6, None], type=pa.int32()), values)
    for values in (
        pa.array([struct_value] * 7, type=struct_type),
        pa.array([[None, None]] * 7, type=list_type),
    )
]
repeated_schema = pa.schema([pa.field(name, column.type) for name, column in zip(("s", "f"), repeated_columns)])
write("repeated", repeated_schema, [pa.record_batch(repeated_columns, schema=repeated_schema)])

value_types = [pa.null(), struct_type, list_type, pa.int32()]
dictionary_schema = pa.schema([
    pa.field(name, pa.dictionary(pa.int32(), value_type))
    for name, value_type in zip(("n", "s", "f", "d"), value_types)
])


def dictionary_batch(values, indices, null_indices=None):
    columns = []
    for pos, (entries, value_type) in enumerate(zip(values, value_types)):
        field_indices = null_indices if pos == 0 and null_indices is not None else indices
        columns.append(pa.DictionaryArray.from_arrays(
            pa.array(field_indices, type=pa.int32()), pa.array(entries, type=value_type)))
    return pa.record_batch(columns, schema=dictionary_schema)


# Delta values add null complex entries and another integer after the base's constant complex entries.
# The stream also starts with an empty dictionary, which PyArrow replaces when the values first appear.
base_values = [[None], [struct_value], [[None, None]], [10]]
file_batches = [
    dictionary_batch(base_values, [None] * 5),
    dictionary_batch(base_values, [0, 0, None, 0]),
    dictionary_batch(
        [[None], [struct_value, None], [[None, None], None], [10, 20]],
        [1, 0, None, 1], null_indices=[0, 0, None, 0]),
]
write("deltas", dictionary_schema, file_batches, deltas=True, formats=("Arrow",))
stream_batches = [dictionary_batch([[], [], [], []], [None] * 5), *file_batches[1:]]
write("deltas", dictionary_schema, stream_batches, deltas=True, formats=("ArrowStream",))

# A replacement dictionary changes the values used by the next record batch.
write("replacement", dictionary_schema, [
    dictionary_batch([[None], [struct_value], [[None, None]], [10]], [0] * 5),
    dictionary_batch([[None], [None], [None], [20]], [0] * 4),
], formats=("ArrowStream",))
PYEOF

write_query()
{
    printf '%s;\n' "$1"
}

COMPLEX_TYPES='s Tuple(a Nullable(Int64), b Array(Nullable(Int8))), f Array(Nullable(Int64))'
ROOT_TYPES="n Nullable(UInt8), ${COMPLEX_TYPES}, e Tuple()"
MIXED_TYPES="id Int64, ${ROOT_TYPES}, v UInt64, t Tuple(a Int64, extra UInt8), missing UInt8"
DICTIONARY_TYPES="n Nullable(UInt8), ${COMPLEX_TYPES}, d Int64"

for FORMAT in Arrow ArrowStream; do
    for BLOCK_SIZE in 2 3; do
        echo "SET max_block_size=${BLOCK_SIZE};"
        echo "SELECT '${FORMAT} max_block_size=${BLOCK_SIZE}';"
        write_query "
            SELECT count(), sum(id), countIf(isNull(n)), countIf(isNull(s.a)), sum(length(s.b)),
                   sum(length(f)), sum(v), sum(t.a), sum(t.extra), sum(missing), countIf(toString(e) = '()'), max(bs)
            FROM
            (
                SELECT *, blockSize() AS bs
                FROM file('${TMP_DIR}/mixed.${FORMAT}', '${FORMAT}', '${MIXED_TYPES}')
            )"
        write_query "
            SELECT count(), countIf(isNull(n)), countIf(isNull(s.a)), sum(length(s.b)),
                   sum(length(f)), countIf(toString(e) = '()'), max(bs)
            FROM
            (
                SELECT *, blockSize() AS bs
                FROM file('${TMP_DIR}/bufferless.${FORMAT}', '${FORMAT}', '${ROOT_TYPES}')
            )"
        write_query "
            SELECT count(), countIf(isNull(s)), sum(length(tupleElement(assumeNotNull(s), 'b'))), max(bs)
            FROM
            (
                SELECT *, blockSize() AS bs
                FROM file('${TMP_DIR}/nullable_struct.${FORMAT}', '${FORMAT}',
                    's Nullable(Tuple(a Nullable(Int64), b Array(Nullable(Int8))))')
            )
            SETTINGS allow_experimental_nullable_tuple_type=1"
        write_query "
            SELECT count(), countIf(isNull(s.a)), sum(length(s.b)), sum(length(f)), max(bs)
            FROM
            (
                SELECT *, blockSize() AS bs
                FROM file('${TMP_DIR}/repeated.${FORMAT}', '${FORMAT}',
                    's Tuple(a Nullable(Int64), b Array(Nullable(Int8))), f Array(Nullable(Int64))')
            )"
        for CASE in deltas replacement; do
            if [[ "$CASE" == replacement && "$FORMAT" == Arrow ]]; then
                continue
            fi
            write_query "
                SELECT count(), countIf(isNull(n)), countIf(isNull(s.a)), sum(length(s.b)),
                       sum(length(f)), sum(d), max(bs)
                FROM
                (
                    SELECT *, blockSize() AS bs
                    FROM file('${TMP_DIR}/${CASE}.${FORMAT}', '${FORMAT}', '${DICTIONARY_TYPES}')
                )"
        done
        write_query "
            SELECT
                (SELECT count() FROM file('${TMP_DIR}/mixed.${FORMAT}', '${FORMAT}', '${MIXED_TYPES}')),
                (SELECT count() FROM file('${TMP_DIR}/empty.${FORMAT}', '${FORMAT}', '${MIXED_TYPES}'))"
    done
done > "$TMP_DIR/queries.sql"

${CLICKHOUSE_LOCAL} --path "$TMP_DIR/local" --max_threads=1 \
    --allow_experimental_nullable_tuple_type=0 --input_format_null_as_default=1 \
    --multiquery --queries-file "$TMP_DIR/queries.sql"
