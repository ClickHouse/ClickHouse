#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: experimental Iceberg writes.
#
# Regression test for the Iceberg part of https://github.com/ClickHouse/ClickHouse/pull/115703:
# enum-like integers coming from Iceberg metadata files (the `status` and `data_file.content` of a
# manifest entry and the `content` of a manifest list entry) were cast to the C++ enums without
# validation, which is undefined behaviour for an out-of-range value. Reading a table whose
# metadata contains such a value must throw `ICEBERG_SPECIFICATION_VIOLATION`.
#
# The test creates a well-formed Iceberg table and then patches a single enum value in the Avro
# metadata (the files are written with the null codec, so the value can be rewritten in place
# with a schema-driven walk that needs no third-party Avro library).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_ROOT="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_malformed_enums"

# Rewrites the value of an int field of the first record of an Avro object container file.
# Usage: patch_avro_int <file> <dotted.field.path> <new_value>
patch_avro_int()
{
    python3 - "$1" "$2" "$3" <<'PY'
import json
import sys

path, target, new_value = sys.argv[1], sys.argv[2], int(sys.argv[3])
data = open(path, 'rb').read()


def read_varint(buf, pos):
    shift = 0
    result = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
    return (result >> 1) ^ -(result & 1), pos


def write_varint(value):
    value = (value << 1) ^ (value >> 63)
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


# Header: magic, metadata map, 16-byte sync marker.
assert data[:4] == b'Obj\x01', 'not an Avro object container file'
pos = 4
meta = {}
while True:
    count, pos = read_varint(data, pos)
    if count == 0:
        break
    if count < 0:
        _, pos = read_varint(data, pos)
        count = -count
    for _ in range(count):
        length, pos = read_varint(data, pos)
        key = data[pos:pos + length].decode()
        pos += length
        length, pos = read_varint(data, pos)
        meta[key] = data[pos:pos + length]
        pos += length
pos += 16
assert meta.get('avro.codec', b'null') == b'null', 'the file is compressed, cannot patch in place'
schema = json.loads(meta['avro.schema'])

# First data block: record count, byte size, records.
block_start = pos
row_count, pos = read_varint(data, pos)
block_size, pos = read_varint(data, pos)
data_start = pos

# Walk the first record following the writer schema until the target field is reached.
# Only the types that can precede the patched enums need to be skipped.
found = None


def walk(node, prefix, pos):
    global found
    if isinstance(node, dict):
        if node['type'] == 'record':
            for field in node['fields']:
                name = field['name'] if not prefix else prefix + '.' + field['name']
                if name == target:
                    assert field['type'] == 'int', 'the target field is not an int'
                    found = pos
                    return pos
                pos = walk(field['type'], name, pos)
                if found is not None:
                    return pos
            return pos
        node = node['type']
    if isinstance(node, list):  # union: branch index, then the branch value
        branch, pos = read_varint(data, pos)
        return walk(node[branch], prefix, pos)
    if node in ('int', 'long'):
        _, pos = read_varint(data, pos)
        return pos
    if node in ('string', 'bytes'):
        length, pos = read_varint(data, pos)
        return pos + length
    if node == 'boolean':
        return pos + 1
    if node == 'null':
        return pos
    raise ValueError(f'cannot skip a field of type {node} before {target}')


walk(schema, '', data_start)
assert found is not None, f'field {target} not found in the first record'
_, after_old = read_varint(data, found)
replacement = write_varint(new_value)
patched = data[found:after_old] != replacement
new_block_size = block_size - (after_old - found) + len(replacement)
open(path, 'wb').write(
    data[:block_start] + write_varint(row_count) + write_varint(new_block_size)
    + data[data_start:found] + replacement + data[after_old:])
assert patched, f'the value of {target} did not change'
PY
}

create_table()
{
    rm -rf "${TABLE_ROOT}"
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS t_malformed_enums;
        SET allow_experimental_insert_into_iceberg = 1;
        CREATE TABLE t_malformed_enums (x Int32) ENGINE = IcebergLocal('${TABLE_ROOT}/');
        INSERT INTO t_malformed_enums VALUES (1), (2), (3);
    "
}

# Expects the read to fail with ICEBERG_SPECIFICATION_VIOLATION and with the field-specific
# diagnostic given as the argument, so that the test stays tied to the intended range check even
# if the file patching goes wrong in a way that trips some other Iceberg validation.
# Usage: check_throws <expected message fragment>
check_throws()
{
    local output
    output=$(${CLICKHOUSE_CLIENT} --query "
        SELECT * FROM t_malformed_enums ORDER BY x
        SETTINGS use_iceberg_metadata_files_cache = 0;
    " 2>&1)
    echo "$output" | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -1
    echo "$output" | grep -oF "$1" | head -1
}

manifest_file() { find "${TABLE_ROOT}/metadata" -name '*.avro' ! -name 'snap-*' | head -1; }
manifest_list() { find "${TABLE_ROOT}/metadata" -name 'snap-*.avro' | head -1; }

echo "-- the table reads fine before the metadata is patched"
create_table
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_malformed_enums ORDER BY x SETTINGS use_iceberg_metadata_files_cache = 0;"

echo "-- out-of-range 'status' of a manifest entry"
create_table
patch_avro_int "$(manifest_file)" 'status' 100
check_throws "unexpected value 100 of 'status' in a manifest file"

# The negative manifest-entry values must be rejected as well. The expected fragment does not pin
# the reported value: today a negative value surfaces as a huge unsigned one, and the exact
# rendering may change while the rejection itself must stay.
echo "-- negative 'status' of a manifest entry"
create_table
patch_avro_int "$(manifest_file)" 'status' -1
check_throws "of 'status' in a manifest file"

echo "-- out-of-range 'data_file.content' of a manifest entry"
create_table
patch_avro_int "$(manifest_file)" 'data_file.content' 100
check_throws "unexpected value 100 of 'data_file.content' in a manifest file"

echo "-- negative 'data_file.content' of a manifest entry"
create_table
patch_avro_int "$(manifest_file)" 'data_file.content' -1
check_throws "of 'data_file.content' in a manifest file"

echo "-- out-of-range 'content' of a manifest list entry"
create_table
patch_avro_int "$(manifest_list)" 'content' 100
check_throws "unexpected value 100 of the field 'content'"

echo "-- negative 'content' of a manifest list entry"
create_table
patch_avro_int "$(manifest_list)" 'content' -1
check_throws "unexpected value -1 of the field 'content'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_malformed_enums;"
rm -rf "${TABLE_ROOT}"
