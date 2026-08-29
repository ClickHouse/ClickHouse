#!/usr/bin/env bash
# Tags: no-fasttest
# Reproduces https://github.com/ClickHouse/ClickHouse/issues/116985
# Reading an Iceberg table through a path that is an ancestor of the table directory, with the
# metadata document named by iceberg_metadata_file_path, resolved every data path against the
# queried path and so dropped the intermediate directories from every key.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR="05048_iceberg_deeper/${CLICKHOUSE_TEST_UNIQUE_NAME}"
# The Iceberg metadata files cache would serve the copy read while the table was created.
NC="--use_iceberg_metadata_files_cache 0"

# t1: metadata `location` is an absolute path (the default).
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg 1 -q "
    DROP TABLE IF EXISTS t1_05048;
    CREATE TABLE t1_05048 (x UInt32, s String) ENGINE = IcebergS3(s3_conn, filename = '${DIR}/t1/sub');
    INSERT INTO t1_05048 VALUES (1, 'a'), (2, 'b'), (3, 'c');
    DROP TABLE t1_05048;
"

echo -n 'A control  '
${CLICKHOUSE_CLIENT} $NC -q "
    SELECT groupArray(x) FROM (SELECT x FROM icebergS3(s3_conn, filename = '${DIR}/t1/sub') ORDER BY x)"

echo -n 'B absolute '
${CLICKHOUSE_CLIENT} $NC -q "
    SELECT groupArray(x) FROM (
        SELECT x FROM icebergS3(s3_conn, filename = '${DIR}/t1',
            SETTINGS iceberg_metadata_file_path = 'sub/metadata/v2.metadata.json') ORDER BY x)"

# t2: metadata `location` is a full URI.
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg 1 --write_full_path_in_iceberg_metadata 1 -q "
    DROP TABLE IF EXISTS t2_05048;
    CREATE TABLE t2_05048 (x UInt32, s String) ENGINE = IcebergS3(s3_conn, filename = '${DIR}/t2/sub');
    INSERT INTO t2_05048 VALUES (1, 'a'), (2, 'b'), (3, 'c');
    DROP TABLE t2_05048;
"

echo -n 'C full-uri '
${CLICKHOUSE_CLIENT} $NC -q "
    SELECT groupArray(x) FROM (
        SELECT x FROM icebergS3(s3_conn, filename = '${DIR}/t2',
            SETTINGS iceberg_metadata_file_path = 'sub/metadata/v2.metadata.json') ORDER BY x)"

# t3: `location` names a directory DEEPER than the one the metadata document sits in, so the
# document's own position is the only sound source for the table root. Re-rooting at the queried
# path must be kept here.
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg 1 -q "
    DROP TABLE IF EXISTS t3_05048;
    CREATE TABLE t3_05048 (x UInt32, s String) ENGINE = IcebergS3(s3_conn, filename = '${DIR}/t3');
    INSERT INTO t3_05048 VALUES (7, 'd'), (8, 'e');
    DROP TABLE t3_05048;
"
# input_format_parallel_parsing=0: the metadata file is pretty-printed and parallel parsing
# returns its lines out of order, which breaks the json.load below.
${CLICKHOUSE_CLIENT} --input_format_parallel_parsing 0 --output_format_parallel_formatting 0 -q "
    SELECT * FROM s3(s3_conn, filename='${DIR}/t3/metadata/v2.metadata.json', structure='line String', format='LineAsString')
" | python3 -c "
import json, sys
m = json.load(sys.stdin)
old = m['location'].rstrip('/')
new = old + '/sub'
m['location'] = new
for s in m.get('snapshots', []):
    ml = s['manifest-list']
    for prefix in [old + '/', old]:
        if ml.startswith(prefix):
            s['manifest-list'] = new + '/' + ml[len(prefix):].lstrip('/')
            break
print(json.dumps(m))
" | ${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3(s3_conn, filename='${DIR}/t3/metadata/v2.metadata.json', structure='line String', format='LineAsString')
    SETTINGS s3_truncate_on_insert=1
    SELECT * FROM input('line String') FORMAT LineAsString
"

echo -n 'D kept     '
${CLICKHOUSE_CLIENT} $NC -q "
    SELECT groupArray(x) FROM (SELECT x FROM icebergS3(s3_conn, filename = '${DIR}/t3') ORDER BY x)"

# Writing to a table whose root had to be derived is refused: everything scoped to the queried
# path would reach the sibling tables under it.
${CLICKHOUSE_CLIENT} $NC -q "
    DROP TABLE IF EXISTS t1_deep_05048;
    CREATE TABLE t1_deep_05048 ENGINE = IcebergS3(s3_conn, filename = '${DIR}/t1')
    SETTINGS iceberg_metadata_file_path = 'sub/metadata/v2.metadata.json';
"
echo -n 'E refused  '
${CLICKHOUSE_CLIENT} $NC --allow_experimental_insert_into_iceberg 1 -q "
    INSERT INTO t1_deep_05048 VALUES (9, 'z')" 2>&1 | grep -o "NOT_IMPLEMENTED" | head -1

# t4: the document is deeper, but `location` still names the queried path, so the two sources
# disagree and the table root must NOT be derived. Reads and writes stay as they are.
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg 1 -q "
    DROP TABLE IF EXISTS t4_05048;
    CREATE TABLE t4_05048 (x UInt32, s String) ENGINE = IcebergS3(s3_conn, filename = '${DIR}/t4');
    INSERT INTO t4_05048 VALUES (5, 'g'), (6, 'h');
    DROP TABLE t4_05048;
"
# A plain copy of the document one level down, with no edit, so `location` is left alone.
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3(s3_conn, filename='${DIR}/t4/archive/metadata/v2.metadata.json', structure='line String', format='LineAsString')
    SETTINGS s3_truncate_on_insert=1, input_format_parallel_parsing=0, output_format_parallel_formatting=0
    SELECT * FROM s3(s3_conn, filename='${DIR}/t4/metadata/v2.metadata.json', structure='line String', format='LineAsString')
"

echo -n 'G kept     '
${CLICKHOUSE_CLIENT} $NC -q "
    SELECT groupArray(x) FROM (
        SELECT x FROM icebergS3(s3_conn, filename = '${DIR}/t4',
            SETTINGS iceberg_metadata_file_path = 'archive/metadata/v2.metadata.json') ORDER BY x)"

${CLICKHOUSE_CLIENT} $NC -q "
    DROP TABLE IF EXISTS t4_arch_05048;
    CREATE TABLE t4_arch_05048 ENGINE = IcebergS3(s3_conn, filename = '${DIR}/t4')
    SETTINGS iceberg_metadata_file_path = 'archive/metadata/v2.metadata.json';
"
echo -n 'G write    '
if ${CLICKHOUSE_CLIENT} $NC --allow_experimental_insert_into_iceberg 1 -q "
    INSERT INTO t4_arch_05048 VALUES (99, 'ok')" > /dev/null 2>&1
then
    echo 'OK'
else
    echo 'REFUSED'
fi

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t1_deep_05048;
    DROP TABLE IF EXISTS t4_arch_05048;
"
