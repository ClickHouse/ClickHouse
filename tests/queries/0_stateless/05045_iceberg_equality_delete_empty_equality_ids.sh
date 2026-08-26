#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `IcebergLocal` needs the USE_AVRO build option.
#
# An Iceberg manifest entry that marks a delete file as an equality delete without naming any
# equality field id is invalid metadata, because the field ids are what equality is defined by.
# Reading such a table must be rejected with `ICEBERG_SPECIFICATION_VIOLATION`.
#
# ClickHouse only ever writes position deletes, so the fixture writes a well-formed table and then
# relabels its position-delete manifest entry as an equality delete. That is one byte:
# `data_file.content` 1 becomes 2, and the manifests use the null codec so it can be rewritten in
# place. The relabelled entry names no equality field id, because ClickHouse writes `equality_ids`
# as the Avro null branch and the manifest reader maps that to an empty list.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap '${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"; rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 -m --query "
    CREATE TABLE ${TABLE} (id UInt64, s String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    ORDER BY (id) SETTINGS iceberg_format_version = 2;
    INSERT INTO ${TABLE} SELECT number, toString(number) FROM numbers(500);
    DELETE FROM ${TABLE} WHERE id < 100;
"

# The table reads correctly before the manifest is touched, so the error below is attributable to
# the patched byte and not to a fixture that never worked.
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM icebergLocal('${TABLE_PATH}') SETTINGS use_iceberg_metadata_files_cache = 0"

python3 - "${TABLE_PATH}metadata" <<'PY'
import glob, re, sys

def varint(buf, pos):
    shift = result = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return (result >> 1) ^ -(result & 1), pos
        shift += 7

patched = 0
for path in sorted(glob.glob(sys.argv[1] + '/*.avro')):
    if '/snap-' in path:   # a manifest list, not a manifest
        continue
    raw = bytearray(open(path, 'rb').read())
    if raw[:4] != b'Obj\x01':
        sys.exit(path + ': not an Avro object container file')

    # Skip the header: magic, then the metadata map, then the 16-byte sync marker.
    pos = 4
    while True:
        count, pos = varint(raw, pos)
        if count == 0:
            break
        if count < 0:
            _, pos = varint(raw, pos)
            count = -count
        for _ in range(count):
            for _ in range(2):     # key, then value
                length, pos = varint(raw, pos)
                pos += length
    header = bytes(raw[:pos])
    pos += 16

    # The writer schema sits in the header as plain JSON. Assert the field order the walk below
    # relies on, and the codec that makes an in-place patch valid, so a change fails loudly here
    # rather than silently patching some other field.
    names = re.findall(rb'"name"\s*:\s*"(\w+)"', header)
    if names[1:6] != [b'status', b'snapshot_id', b'sequence_number', b'file_sequence_number', b'data_file']:
        sys.exit(path + ': unexpected manifest_entry field order')
    if names[7] != b'content':
        sys.exit(path + ': data_file does not start with content')
    if b'equality_ids' not in header:
        sys.exit(path + ': the manifest has no equality_ids field, so it is not format version 2')
    if not re.search(rb'avro\.codec\x08null', header):
        sys.exit(path + ': the manifest is compressed, it cannot be patched in place')

    # Then the first block's row count and byte size, and the first record up to data_file.content.
    for _ in range(3):             # block row count, block byte size, status
        _, pos = varint(raw, pos)
    for _ in range(3):             # the three ["null", "long"] fields
        branch, pos = varint(raw, pos)
        if branch == 1:
            _, pos = varint(raw, pos)

    content, after = varint(raw, pos)
    if content != 1:               # not the position-delete manifest
        continue
    if after - pos != 1:
        sys.exit(path + ': content is not a one-byte varint, patching it would move the record')
    raw[pos] = 0x04                # 2, zigzag encoded
    open(path, 'wb').write(bytes(raw))
    patched += 1

if patched == 0:
    sys.exit('no manifest entry describing a position-delete file was found')
PY

${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM icebergLocal('${TABLE_PATH}') SETTINGS use_iceberg_metadata_files_cache = 0" 2>&1 \
    | grep -oF 'ICEBERG_SPECIFICATION_VIOLATION' | head -n1

# The server is still alive.
${CLICKHOUSE_CLIENT} --query "SELECT 1"
