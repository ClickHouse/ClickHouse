#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: `Parquet` format is not supported in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

python3 - "$WORK_DIR" <<'PY'
import struct
import sys

work = sys.argv[1]

class W:
    def __init__(self):
        self.out = bytearray()

    def u8(self, value):
        self.out.append(value & 0xff)

    def varint(self, value):
        value &= (1 << 64) - 1
        while True:
            byte = value & 0x7f
            value >>= 7
            self.out.append(byte | 0x80 if value else byte)
            if not value:
                return

    def zz(self, value):
        self.varint(((value << 1) ^ (value >> 63)) & ((1 << 64) - 1))

CT_I32, CT_I64, CT_BIN, CT_LIST, CT_STRUCT = 5, 6, 8, 9, 12
REQUIRED = 0
INT32, INT64, BYTE_ARRAY = 1, 2, 6

def field(writer, last, field_id, compact_type):
    delta = field_id - last
    if 1 <= delta <= 15:
        writer.u8((delta << 4) | compact_type)
    else:
        writer.u8(compact_type)
        writer.zz(field_id)
    return field_id

def logical_variant(writer):
    last = 0
    last = field(writer, last, 16, CT_STRUCT)  # LogicalType::VARIANT
    writer.u8(0)  # empty VariantType
    writer.u8(0)  # LogicalType stop

def schema_elem(writer, name, repetition=None, num_children=None, physical_type=None, variant=False):
    last = 0
    if physical_type is not None:
        last = field(writer, last, 1, CT_I32)
        writer.zz(physical_type)
    if repetition is not None:
        last = field(writer, last, 3, CT_I32)
        writer.zz(repetition)
    last = field(writer, last, 4, CT_BIN)
    writer.varint(len(name))
    writer.out += name.encode()
    if num_children is not None:
        last = field(writer, last, 5, CT_I32)
        writer.zz(num_children)
    if variant:
        last = field(writer, last, 10, CT_STRUCT)  # SchemaElement::logicalType
        logical_variant(writer)
    writer.u8(0)

footer = W()
last = 0
last = field(footer, last, 1, CT_I32)
footer.zz(2)  # FileMetaData::version
last = field(footer, last, 2, CT_LIST)
footer.u8((8 << 4) | CT_STRUCT)  # FileMetaData::schema

schema_elem(footer, "root", num_children=1)
schema_elem(footer, "v", repetition=REQUIRED, num_children=3, variant=True)
schema_elem(footer, "metadata", repetition=REQUIRED, physical_type=BYTE_ARRAY)
schema_elem(footer, "value", repetition=REQUIRED, physical_type=BYTE_ARRAY)
schema_elem(footer, "typed_value", repetition=REQUIRED, num_children=1)
schema_elem(footer, "a", repetition=REQUIRED, num_children=2)
schema_elem(footer, "value", repetition=REQUIRED, physical_type=INT32)
schema_elem(footer, "typed_value", repetition=REQUIRED, physical_type=INT64)

last = field(footer, last, 3, CT_I64)
footer.zz(0)  # FileMetaData::num_rows
last = field(footer, last, 4, CT_LIST)
footer.u8((0 << 4) | CT_STRUCT)  # FileMetaData::row_groups
footer.u8(0)

metadata = bytes(footer.out)
with open(f"{work}/bad_nested_wrapper.parquet", "wb") as out:
    out.write(b"PAR1" + metadata + struct.pack("<I", len(metadata)) + b"PAR1")
PY

out=$("${CLICKHOUSE_LOCAL}" --query "
    DESC file('${WORK_DIR}/bad_nested_wrapper.parquet', Parquet)
    SETTINGS
        enable_json_type = 1,
        input_format_parquet_use_native_reader_v3 = 1" 2>&1) && status=0 || status=$?

if [[ $status -ne 0 ]] && grep -q 'primitive payload must be `BYTE_ARRAY`' <<< "$out"; then
    echo "malformed nested wrapper: rejected"
else
    echo "malformed nested wrapper: unexpected result"
    echo "$out"
    exit 1
fi
