#!/usr/bin/env bash
# Tags: no-fasttest
# Reproduces the IcebergLocal report in https://github.com/ClickHouse/ClickHouse/issues/102321
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
# names, and all four derive from the one location, so each is asserted separately. Each names the
# whole expected path and not just the scheme, because a table under the wrong root still reads
# back: the resolver maps the shared metadata prefix onto the configured table root.
python3 -c "
import json
root = 'file://' + '${TEST_DIR}/t1/'
loc = json.load(open('${TEST_DIR}/t0/metadata/v2.metadata.json'))['location']
print('A default   ' + ('no-scheme' if '://' not in loc else 'UNEXPECTED ' + loc))
m = json.load(open('${TEST_DIR}/t1/metadata/v2.metadata.json'))
loc = m['location']
print('B location  ' + ('file:///+root' if loc == root else 'UNEXPECTED ' + loc))
# An empty authority contributes no path segment, so an absolute path keeps exactly one root slash.
print('C authority ' + ('one-slash' if loc.startswith('file:///') and not loc.startswith('file:////') else 'UNEXPECTED ' + loc))
ml = m['snapshots'][-1]['manifest-list']
print('D manifests ' + ('file:///+metadata' if ml.startswith(root + 'metadata/') else 'UNEXPECTED ' + ml))
"

# E-F reach the Avro files by their position on disk rather than by the path the document names,
# so the strings inside them are read the same way whatever spelling the document carries.
SNAPSHOT_AVRO=$(ls "${TEST_DIR}"/t1/metadata/snap-*.avro)
MANIFEST_AVRO=$(ls "${TEST_DIR}"/t1/metadata/*.avro | grep -v '/snap-')
echo -n 'E manifest  '
${CLICKHOUSE_CLIENT} -q "
    SELECT if(startsWith(manifest_path, 'file://${TEST_DIR}/t1/metadata/'), 'file:///+metadata', 'UNEXPECTED ' || manifest_path)
    FROM file('${SNAPSHOT_AVRO}', 'Avro')"

echo -n 'F datafile  '
${CLICKHOUSE_CLIENT} -q "
    SELECT if(startsWith(data_file.file_path, 'file://${TEST_DIR}/t1/data/'), 'file:///+data', 'UNEXPECTED ' || data_file.file_path)
    FROM file('${MANIFEST_AVRO}', 'Avro')"

echo -n 'G readback  '
${CLICKHOUSE_CLIENT} $NC -q "SELECT count(), sum(v) FROM icebergLocal('${TEST_DIR}/t1/')"

# H: a metadata document that declares the retired `local://` spelling still reads, so the resolver
# keeps accepting a location it would no longer write. Only `location` and `manifest-list` carry the
# retired spelling here while the Avro files keep the new `file:///` paths, which reaches more of the
# resolver than a uniformly retired document would: `manifest-list` matches the declared location and
# takes its prefix-match branch, the Avro paths do not and take the fallbacks. A table whose four
# carriers all hold the retired spelling is covered by the before/after measurement in the pull
# request, not here. This arm passes on both sides of the change.
python3 -c "
import json
p = '${TEST_DIR}/t1/metadata/v2.metadata.json'
m = json.load(open(p))
m['location'] = m['location'].replace('file:///', 'local:////', 1)
for s in m.get('snapshots', []):
    s['manifest-list'] = s['manifest-list'].replace('file:///', 'local:////', 1)
json.dump(m, open('${TEST_DIR}/t1/metadata/v3.metadata.json', 'w'))
"
# v2 leaves the candidate set, which is globbed by the `.metadata.json` suffix, so the planted v3 is
# the only document that can be selected. v1 predates the INSERT, so a reader that fell back to it
# would report a different row count instead of v2's identical one.
mv "${TEST_DIR}/t1/metadata/v2.metadata.json" "${TEST_DIR}/t1/metadata/v2.metadata.json.bak"
echo -n 'H oldform   '
${CLICKHOUSE_CLIENT} $NC -q "SELECT count(), sum(v) FROM icebergLocal('${TEST_DIR}/t1/')"

# I: the setting is read when the table is created, so a second INSERT with it on must not restamp
# t0. This arm pins pre-existing semantics and therefore holds on both sides of the change.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --write_full_path_in_iceberg_metadata 1 -q "
    INSERT INTO t0_05060 VALUES (3, 30);
"
python3 -c "
import json
loc = json.load(open('${TEST_DIR}/t0/metadata/v3.metadata.json'))['location']
print('I setting-at-insert ' + ('no-scheme' if '://' not in loc else 'UNEXPECTED ' + loc))
"

# J: a relative engine argument. Its data sits under the process directory, while the location is
# made absolute by the storage-relative normalization that runs whatever the setting is, so the
# stamped path names a directory the table does not occupy. Both spellings are read here, so the
# arm pins the setting contributing the scheme and nothing else, and reddens if either side moves.
# A server resolves a relative argument against `user_files_path` and rejects it, so this runs
# under `clickhouse-local`, whose object storage is rooted at `/`.
mkdir -p "${TEST_DIR}/rel0" "${TEST_DIR}/rel1"
(
    cd "${TEST_DIR}" || exit 1
    ${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg 1 -q "
        CREATE TABLE r0 (id UInt64, v Int64) ENGINE = IcebergLocal('rel0/');
        INSERT INTO r0 VALUES (1, 10), (2, 20);
    " < /dev/null
    ${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg 1 --write_full_path_in_iceberg_metadata 1 -q "
        CREATE TABLE r1 (id UInt64, v Int64) ENGINE = IcebergLocal('rel1/');
        INSERT INTO r1 VALUES (1, 10), (2, 20);
    " < /dev/null
)
python3 -c "
import json
off = json.load(open('${TEST_DIR}/rel0/metadata/v2.metadata.json'))['location']
on = json.load(open('${TEST_DIR}/rel1/metadata/v2.metadata.json'))['location']
scheme_only = on == 'file://' + off.replace('rel0', 'rel1')
print('J relative  ' + (off.replace('rel0', 'rel') + ' ' + on.replace('rel1', 'rel')
                        if scheme_only else 'UNEXPECTED ' + off + ' ' + on))
"

# K names the other half of the same mismatch, so J's stamped value is pinned against where the
# data really is rather than on its own, and reading it back shows the table stays readable here.
echo -n 'K datapath  '
(
    cd "${TEST_DIR}" || exit 1
    ${CLICKHOUSE_LOCAL} -q "
    SELECT DISTINCT if(startsWith(_path, '/'), 'UNEXPECTED ' || _path, 'relative ' || toString(count() OVER ()))
    FROM icebergLocal('rel1/');
    " < /dev/null
)
