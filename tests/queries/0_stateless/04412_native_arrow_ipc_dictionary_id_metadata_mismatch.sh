#!/usr/bin/env bash
# Tags: no-fasttest
# This test requires PyArrow to write schemas with shared dictionary identifiers.
# Dictionary fields must agree on UUID interpretation and the names of struct and union elements.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PY'
from pathlib import Path
import struct
import sys

import pyarrow as pa

out = Path(sys.argv[1])


def indirect(data, position):
    return position + struct.unpack_from("<I", data, position)[0]


def field(data, table, index):
    vtable = table - struct.unpack_from("<i", data, table)[0]
    return table + struct.unpack_from("<H", data, vtable + 4 + 2 * index)[0]


# Schema-only inputs check compatibility before any dictionary values or row indices are decoded.
for name, left, right in (
    ("uuid_extension", pa.field("a", pa.dictionary(pa.int32(), pa.binary(16)),
                                metadata={"ARROW:extension:name": "arrow.uuid"}),
                       pa.field("b", pa.dictionary(pa.int32(), pa.binary(16)))),
    ("uuid_logical_type", pa.field("a", pa.dictionary(pa.int32(), pa.binary(16)),
                                   metadata={"PARQUET:logical_type": "UUID"}),
                          pa.field("b", pa.dictionary(pa.int32(), pa.binary(16)))),
    ("struct_names", pa.field("a", pa.dictionary(pa.int32(), pa.struct([("x", pa.int32())]))),
                     pa.field("b", pa.dictionary(pa.int32(), pa.struct([("y", pa.int32())])))),
    ("union_names", pa.field("a", pa.dictionary(pa.int32(), pa.union([pa.field("x", pa.int32())], mode="dense"))),
                    pa.field("b", pa.dictionary(pa.int32(), pa.union([pa.field("y", pa.int32())], mode="dense")))),
):
    data = bytearray(pa.schema([left, right]).serialize())
    message = indirect(data, 8)
    schema = indirect(data, field(data, message, 2))
    fields = indirect(data, field(data, schema, 1))
    second = indirect(data, fields + 8)
    encoding = indirect(data, field(data, second, 4))
    struct.pack_into("<q", data, field(data, encoding, 0), 0)
    data.extend(struct.pack("<II", 0xFFFFFFFF, 0))
    (out / f"{name}.arrows").write_bytes(data)
PY

for CASE in uuid_extension uuid_logical_type struct_names union_names; do
    cat <<SQL
SELECT * FROM file('${TMP_DIR}/${CASE}.arrows', 'ArrowStream') FORMAT Null; -- { serverError INCORRECT_DATA }
SELECT 'Rejected ${CASE}';
SQL
done > "$TMP_DIR/queries.sql"

${CLICKHOUSE_LOCAL} --path "$TMP_DIR/local" --max_threads=1 \
    --multiquery --queries-file "$TMP_DIR/queries.sql"
