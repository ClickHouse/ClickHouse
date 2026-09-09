#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: experimental Iceberg writes.
#
# The metadata-only `count()` sums the per-data-file `record_count` of the manifest files, and
# cross-checks the sum against the snapshot summary's `total-records`. Neither side is measured
# against the data by every writer:
# - the summary is maintained incrementally (parent total plus this commit's delta), so a
#   corrupted commit in the table history poisons every later snapshot;
# - ClickHouse itself used to stamp the summary's `added-records` into the `record_count` of
#   *every* data file of a manifest it generated (https://github.com/ClickHouse/ClickHouse/issues/104321,
#   fixed in 26.5), so for a manifest describing N data files the sum comes out N times too
#   large. Such tables are out there and cannot be repaired retroactively, and `SELECT count()`
#   on one of them returned N times the real row count.
# A disagreement between the two proves that one of them is corrupted, without telling which
# one, so count() must refuse the metadata-only count and count the rows of the data files.
#
# Each case writes a well-formed table of three single-row data files and then corrupts one of
# the two row-count sources; the first case corrupts neither and pins that the metadata-only
# count is still applied to a healthy table. The manifest files are written with the null codec,
# so a value can be rewritten in place with a schema-driven walk that needs no third-party Avro
# library.

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

# Rewrites `total-records` in the summary of the current snapshot of the latest metadata file.
# Usage: patch_summary_total_records <table root> <expected old value> <new value>
patch_summary_total_records()
{
    python3 - "$1" "$2" "$3" <<'PY'
import glob
import json
import re
import sys

root, old_expected, new_value = sys.argv[1], sys.argv[2], sys.argv[3]
files = glob.glob(root + '/metadata/*.metadata.json')
assert files, 'no metadata files found'


def version(path):
    match = re.search(r'(\d+)[^/]*\.metadata\.json$', path)
    return int(match.group(1)) if match else -1


latest = max(files, key=version)
with open(latest) as f:
    metadata = json.load(f)
patched = False
for snapshot in metadata['snapshots']:
    if snapshot['snapshot-id'] == metadata['current-snapshot-id']:
        assert snapshot['summary']['total-records'] == old_expected, snapshot['summary']
        snapshot['summary']['total-records'] = new_value
        patched = True
assert patched, 'the current snapshot was not found in the latest metadata file'
with open(latest, 'w') as f:
    json.dump(metadata, f)
PY
}

# One row per data file, so the number of data files does not depend on randomized block sizes.
create_table()
{
    rm -rf "$2"
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS $1;
        SET allow_experimental_insert_into_iceberg = 1;
        SET iceberg_insert_max_rows_in_data_file = 1;
        SET max_insert_threads = 1, max_block_size = 1, max_insert_block_size = 1;
        SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
        CREATE TABLE $1 (x Int32) ENGINE = IcebergLocal('$2/');
        INSERT INTO $1 VALUES (1), (2), (3);
    "
}

# count() with the metadata-only optimization enabled and disabled (pinned: the test runner
# randomizes the setting), the row count the manifest files describe, and whether the
# optimization was applied. The Iceberg metadata caches are disabled because the metadata files
# are patched behind the server's back, and they are immutable per the specification.
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
}

ROOT="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}"

echo '-- trivial_count scan_count manifest_rows optimization_applied'

echo '-- consistent metadata: the metadata-only count is used'
create_table t_consistent "${ROOT}_consistent"
report t_consistent

# The value the buggy writer stamped into every entry. The manifest files then describe
# 3 + 1 + 1 = 5 rows while the snapshot summary claims 3.
echo '-- the manifest files over-report: count() must not return 5'
create_table t_manifest "${ROOT}_manifest"
patch_manifest_record_count "$(find "${ROOT}_manifest/metadata" -name '*.avro' ! -name 'snap-*' | head -1)" 1 3
report t_manifest

echo '-- the snapshot summary over-reports: count() must not return 100'
create_table t_summary "${ROOT}_summary"
patch_summary_total_records "${ROOT}_summary" 3 100
report t_summary

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_consistent; DROP TABLE t_manifest; DROP TABLE t_summary;"
rm -rf "${ROOT}_consistent" "${ROOT}_manifest" "${ROOT}_summary"
