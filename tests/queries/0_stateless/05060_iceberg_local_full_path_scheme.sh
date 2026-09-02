#!/usr/bin/env bash
# Tags: no-fasttest
# Reproduces https://github.com/ClickHouse/ClickHouse/issues/102321
# With write_full_path_in_iceberg_metadata, an IcebergLocal table was stamped with a
# `local://` location and an empty authority in front of an already absolute path, so every
# path in its metadata read `local:////abs/path` and no external Iceberg reader resolved it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

if [ ! -d "${USER_FILES_PATH}" ]; then
    USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
fi

TEST_DIR=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t0_05060; DROP TABLE IF EXISTS t1_05060"
    rm -rf "${TEST_DIR}"
}
trap cleanup EXIT
rm -rf "${TEST_DIR}"
mkdir -p "${TEST_DIR}"

# The metadata files cache would serve the copy read while the table was created.
NC="--use_iceberg_metadata_files_cache 0"

# t0: the default. t1: the same table with the setting on.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 -q "
    CREATE TABLE t0_05060 (id UInt64, v Int64) ENGINE = IcebergLocal('${TEST_DIR}/t0/');
    INSERT INTO t0_05060 VALUES (1, 10), (2, 20);
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --write_full_path_in_iceberg_metadata 1 -q "
    CREATE TABLE t1_05060 (id UInt64, v Int64) ENGINE = IcebergLocal('${TEST_DIR}/t1/');
    INSERT INTO t1_05060 VALUES (1, 10), (2, 20);
"

# A-D read the strings the two tables were stamped with. `location` and `manifest-list` sit in
# the metadata document; `manifest_path` and `data_file.file_path` sit in the Avro files it
# names, and all four derive from the one location, so each is asserted separately.
python3 -c "
import json, sys
loc = json.load(open('${TEST_DIR}/t0/metadata/v2.metadata.json'))['location']
print('A default   ' + ('no-scheme' if '://' not in loc else 'UNEXPECTED ' + loc))
m = json.load(open('${TEST_DIR}/t1/metadata/v2.metadata.json'))
loc = m['location']
print('B location  ' + ('file:///' if loc.startswith('file:///') else 'UNEXPECTED ' + loc))
# An empty authority contributes no path segment, so an absolute path keeps exactly one root slash.
print('C authority ' + ('one-slash' if loc.startswith('file:///') and not loc.startswith('file:////') else 'UNEXPECTED ' + loc))
ml = m['snapshots'][-1]['manifest-list']
print('D manifests ' + ('file:///' if ml.startswith('file:///') else 'UNEXPECTED ' + ml))
"

# E-F reach the Avro files by their position on disk rather than by the path the document names,
# so the strings inside them are read the same way whatever spelling the document carries.
SNAPSHOT_AVRO=$(ls "${TEST_DIR}"/t1/metadata/snap-*.avro)
MANIFEST_AVRO=$(ls "${TEST_DIR}"/t1/metadata/*.avro | grep -v '/snap-')
echo -n 'E manifest  '
${CLICKHOUSE_CLIENT} -q "
    SELECT if(startsWith(manifest_path, 'file:///'), 'file:///', 'UNEXPECTED ' || manifest_path)
    FROM file('${SNAPSHOT_AVRO}', 'Avro')"

echo -n 'F datafile  '
${CLICKHOUSE_CLIENT} -q "
    SELECT if(startsWith(data_file.file_path, 'file:///'), 'file:///', 'UNEXPECTED ' || data_file.file_path)
    FROM file('${MANIFEST_AVRO}', 'Avro')"

echo -n 'G readback  '
${CLICKHOUSE_CLIENT} $NC -q "SELECT count(), sum(v) FROM icebergLocal('${TEST_DIR}/t1/')"

# H: a table whose metadata declares the retired `local://` spelling still reads. Tables written
# before this changed carry it, so the resolver must keep accepting a location it would no longer
# write; this arm passes on both sides of the change.
python3 -c "
import json
p = '${TEST_DIR}/t1/metadata/v2.metadata.json'
m = json.load(open(p))
m['location'] = m['location'].replace('file:///', 'local:////', 1)
for s in m.get('snapshots', []):
    s['manifest-list'] = s['manifest-list'].replace('file:///', 'local:////', 1)
json.dump(m, open('${TEST_DIR}/t1/metadata/v3.metadata.json', 'w'))
"
echo -n 'H oldform   '
${CLICKHOUSE_CLIENT} $NC -q "SELECT count(), sum(v) FROM icebergLocal('${TEST_DIR}/t1/')"
