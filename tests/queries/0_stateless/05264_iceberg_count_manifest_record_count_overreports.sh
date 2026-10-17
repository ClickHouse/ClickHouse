#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: experimental Iceberg writes.
#
# ClickHouse itself used to stamp the snapshot summary's `added-records` into the `record_count`
# of *every* data file of a manifest it generated (https://github.com/ClickHouse/ClickHouse/issues/104321,
# fixed in 26.5), so for a manifest describing N data files the sum of the per-file row counts
# comes out N times too large. Such tables are out there and cannot be repaired retroactively,
# and `SELECT count()` on one of them returned N times the real row count. The metadata-only
# `count()` is answered from the snapshot summary when it provides a usable row count, and the
# sum of the manifest files is only used (cross-checked against the summary's `total-records`)
# when it does not, so the over-reported per-file row counts must not leak into `count()`.
#
# Each case writes a well-formed table of three single-row data files; the second one then
# corrupts the per-file row count of a manifest entry. The manifest files are written with the
# null codec, so a value can be rewritten in place with a schema-driven walk that needs no
# third-party Avro library.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Rewrites `data_file.record_count` of the first manifest entry of an Avro manifest file.
# Usage: patch_manifest_record_count <file> <expected old value> <new value>
patch_manifest_record_count()
{
    python3 - "$1" "$2" "$3" <<'PY'
import json
import sys

path, old_expected, new_value = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
target = 'data_file.record_count'
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
# Only the types that can precede the patched field need to be skipped.
found = None


def walk(node, prefix, pos):
    global found
    if isinstance(node, dict):
        if node['type'] == 'record':
            for field in node['fields']:
                name = field['name'] if not prefix else prefix + '.' + field['name']
                if name == target:
                    assert field['type'] == 'long', 'the target field is not a long'
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
old_value, after_old = read_varint(data, found)
assert old_value == old_expected, f'{target} is {old_value}, expected {old_expected}'
replacement = write_varint(new_value)
new_block_size = block_size - (after_old - found) + len(replacement)
open(path, 'wb').write(
    data[:block_start] + write_varint(row_count) + write_varint(new_block_size)
    + data[data_start:found] + replacement + data[after_old:])
PY
}

# One row per data file: each row is a separate INSERT, so the number of data files depends
# neither on randomized block sizes nor on the test runner batching the inserts asynchronously
# (`async_insert` collapses the rows of consecutive inserts into a single data file).
create_table()
{
    rm -rf "$2"
    ${CLICKHOUSE_CLIENT} --async_insert=0 --query "
        DROP TABLE IF EXISTS $1;
        SET allow_experimental_insert_into_iceberg = 1;
        CREATE TABLE $1 (x Int32) ENGINE = IcebergLocal('$2/');
        INSERT INTO $1 VALUES (1);
        INSERT INTO $1 VALUES (2);
        INSERT INTO $1 VALUES (3);
    "
}

# count() with the metadata-only optimization enabled and disabled (pinned: the test runner
# randomizes the setting), the row count the manifest files describe, whether the optimization
# was applied, and whether `DROP TABLE ... IF EMPTY` refuses the table: it must, since every
# table holds three rows. The Iceberg metadata caches are disabled because
# the metadata files are patched behind the server's back, and they are immutable per the
# specification.
report()
{
    ${CLICKHOUSE_CLIENT} --send_logs_level=fatal --use_iceberg_metadata_files_cache=0 --query "
        SELECT
            (SELECT count() FROM $1 SETTINGS optimize_trivial_count_query = 1) AS trivial_count,
            (SELECT count() FROM $1 SETTINGS optimize_trivial_count_query = 0) AS scan_count,
            (SELECT sum(record_count) FROM system.iceberg_files
             WHERE database = currentDatabase() AND table = '$1' AND content = 0) AS manifest_rows,
            (SELECT count() FROM (EXPLAIN SELECT count() FROM $1 SETTINGS optimize_trivial_count_query = 1)
             WHERE explain LIKE '%Optimized trivial count%') AS optimization_applied
        FORMAT TSV;
    "
    ${CLICKHOUSE_CLIENT} --send_logs_level=fatal --use_iceberg_metadata_files_cache=0 \
        --query "DROP TABLE IF EMPTY $1 SETTINGS ignore_drop_queries_probability = 0" 2>&1 | grep -o 'TABLE_NOT_EMPTY'
}

ROOT="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}"

echo '-- trivial_count scan_count manifest_rows optimization_applied, then the DROP TABLE IF EMPTY refusal'

echo '-- consistent metadata: the metadata-only count is used'
create_table t_consistent "${ROOT}_consistent"
report t_consistent

# The value the buggy writer stamped into every entry. The manifest files then describe
# 3 + 1 + 1 = 5 rows while the snapshot summary claims 3.
echo '-- the manifest files over-report: count() must not return 5, the snapshot summary is used'
create_table t_manifest "${ROOT}_manifest"
patch_manifest_record_count "$(find "${ROOT}_manifest/metadata" -name '*.avro' ! -name 'snap-*' | head -1)" 1 3
report t_manifest

# Some writers omit the delete totals from the snapshot summary, and then it does not provide a
# usable row count: the manifest files are summed, and their sum is cross-checked against the
# summary's `total-records`. They disagree, so the rows of the data files are counted instead.
echo '-- the manifest files over-report and the summary has no delete totals: the rows are counted from the data files'
create_table t_no_delete_totals "${ROOT}_no_delete_totals"
patch_manifest_record_count "$(find "${ROOT}_no_delete_totals/metadata" -name '*.avro' ! -name 'snap-*' | head -1)" 1 3
python3 - "${ROOT}_no_delete_totals/metadata" <<'PY'
import glob
import json
import sys

for path in glob.glob(sys.argv[1] + '/*.metadata.json'):
    metadata = json.load(open(path))
    for snapshot in metadata.get('snapshots', []):
        for key in ('total-position-deletes', 'total-equality-deletes'):
            snapshot.get('summary', {}).pop(key, None)
    json.dump(metadata, open(path, 'w'))
PY
report t_no_delete_totals

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_consistent; DROP TABLE t_manifest; DROP TABLE t_no_delete_totals;"
rm -rf "${ROOT}_consistent" "${ROOT}_manifest" "${ROOT}_no_delete_totals"
