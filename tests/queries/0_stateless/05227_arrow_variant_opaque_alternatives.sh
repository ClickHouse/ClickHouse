#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An alternative of a `Variant` whose type has no Arrow mapping is written as an opaque union child, tagged
# `clickhouse.opaque` with the ClickHouse type it came from. Reading it back needs the tag, because a union
# child's type is settled while decoding: without it every opaque child is `String`, so two of them collapse
# into one `Variant` alternative, and one alongside any other type yields a `Variant` that does not match
# what was asked for.
#
# The Arrow type of each child states its own encoding, so a union can mix the two: in `text` mode an
# `AggregateFunction` alternative is still `Binary`, its text form not being text.

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
COMMON="output_format_arrow_compression_method = 'none', engine_file_truncate_on_insert = 1"
TYPES="a Variant(JSON, String), b Variant(QBit(BFloat16, 3), UInt64), c Variant(AggregateFunction(sum, UInt64), String)"

# Row 0 selects the opaque alternative of each column, row 1 a plain one, row 2 is NULL - a `Variant` NULL
# is its own Arrow child, so it must stay out of the opaque children's rows.
values() {
    echo "SELECT multiIf(number = 0, CAST('{\"x\":1}'::JSON, 'Variant(JSON, String)'),
                         number = 1, CAST('plain', 'Variant(JSON, String)'),
                         CAST(NULL, 'Variant(JSON, String)')) AS a,
                 multiIf(number = 0, CAST([1,2,3]::QBit(BFloat16, 3), 'Variant(QBit(BFloat16, 3), UInt64)'),
                         number = 1, CAST(7::UInt64, 'Variant(QBit(BFloat16, 3), UInt64)'),
                         CAST(NULL, 'Variant(QBit(BFloat16, 3), UInt64)')) AS b,
                 multiIf(number = 0, CAST((SELECT sumState(toUInt64(9))), 'Variant(AggregateFunction(sum, UInt64), String)'),
                         number = 1, CAST('plain', 'Variant(AggregateFunction(sum, UInt64), String)'),
                         CAST(NULL, 'Variant(AggregateFunction(sum, UInt64), String)')) AS c
          FROM numbers(3)"
}

projection() {
    echo "variantType(a) AS a_type, toString(a) AS a_value,
          variantType(b) AS b_type, toString(b) AS b_value,
          variantType(c) AS c_type,
          finalizeAggregation(c.\`AggregateFunction(sum, UInt64)\`) AS c_value"
}

read_back() {
    echo "SELECT '$1' AS mode, $(projection) FROM file('$2', 'ArrowStream', '${TYPES}') FORMAT Vertical;"
}

echo "=== written as binary and as text, read back into the same Variants ==="
${CLICKHOUSE_LOCAL} --multiquery --query "
    INSERT INTO FUNCTION file('${FILE}.binary', 'ArrowStream') $(values)
        SETTINGS ${COMMON}, output_format_arrow_unsupported_types = 'binary';
    INSERT INTO FUNCTION file('${FILE}.text', 'ArrowStream') $(values)
        SETTINGS ${COMMON}, output_format_arrow_unsupported_types = 'text';
    $(read_back binary "${FILE}.binary")
    $(read_back text "${FILE}.text")
    SELECT 'original' AS mode, $(projection) FROM ($(values)) FORMAT Vertical;
    -- A tag naming a type the request does not list is not acted on, and the payload stays the raw bytes.
    SELECT 'untagged request' AS mode, variantType(b) AS b_type, hex(toString(b)) AS b_value
    FROM file('${FILE}.binary', 'ArrowStream', 'b Variant(String, UInt64)') LIMIT 1 FORMAT Vertical;"

# Not acting on the tag leaves the child as the `String` its Arrow type names, so a request that omits the
# tagged type while listing `String` puts two children on one `Variant` alternative. That is rejected, as it
# is for any two Arrow union children sharing a ClickHouse type - a `utf8` and a `large_utf8` child collide
# the same way, with no opaque column involved.
echo "=== a request omitting the tagged type, where String is already taken ==="
${CLICKHOUSE_LOCAL} --query "
    SELECT a FROM file('${FILE}.binary', 'ArrowStream', 'a Variant(String, UInt64)');" 2>&1 |
    grep -o 'multiple children mapping to the same ClickHouse type' | head -1

# A slot no row selects holds undefined bytes, which the opaque path must not read. Neither shape is written
# by ClickHouse, so build them here: a sparse union, which gives every child a slot per row, and a dense one
# whose child keeps a row nothing points at. The payload is taken from a file ClickHouse just wrote, so this
# does not pin down the encoding itself.
echo "=== an unselected slot holding undefined bytes is not read ==="
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${FILE}.one', 'ArrowStream')
        SELECT CAST('{\"x\":1}'::JSON, 'Variant(JSON, UInt64)') AS v SETTINGS ${COMMON};"

python3 - "${FILE}" <<'PY'
import sys
import pyarrow as pa

stem = sys.argv[1]
with pa.ipc.open_stream(f"{stem}.one") as reader:
    batch = reader.read_next_batch()
    field = batch.schema.field(0)
    opaque = field.type.field(0)
    payload = batch.column(0).field(0)[0].as_py()

garbage = b"\xff\xff\xff\xff"
other = pa.field("UInt64", pa.uint64(), nullable=False)
child = pa.array([payload, garbage], type=pa.binary())
types = pa.py_buffer(bytes([0, 1]))

def write(suffix, union_type, buffers, children):
    array = pa.UnionArray.from_buffers(union_type, 2, buffers, children=children)
    writer = pa.ipc.new_stream(f"{stem}.{suffix}", pa.schema([pa.field("v", union_type)]))
    writer.write(pa.record_batch([array], names=["v"]))
    writer.close()

write("dense_extra", pa.dense_union([opaque, other], [0, 1]),
      [None, types, pa.py_buffer(b"".join(x.to_bytes(4, "little") for x in (0, 0)))],
      [child, pa.array([7], type=pa.uint64())])
write("sparse", pa.sparse_union([opaque, other], [0, 1]),
      [None, types],
      [child, pa.array([0, 7], type=pa.uint64())])
PY

for shape in dense_extra sparse; do
    ${CLICKHOUSE_LOCAL} --query "
        SELECT '${shape}' AS shape, variantType(v) AS type, toString(v) AS value
        FROM file('${FILE}.${shape}', 'ArrowStream', 'v Variant(JSON, UInt64)');"
done

rm -f "${FILE}".*
