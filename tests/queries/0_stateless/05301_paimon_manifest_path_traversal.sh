#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/paimon_path_traversal"
rm -rf "${BASE_DIR}"
mkdir -p "${BASE_DIR}/outside"
echo "SECRET_OUTSIDE_TABLE" > "${BASE_DIR}/outside/secret.txt"

# $1 - table name, $2 - manifest file name in the manifest list, $3 - data file name in the manifest
create_table()
{
    local table_dir="${BASE_DIR}/$1"
    mkdir -p "${table_dir}/schema" "${table_dir}/snapshot" "${table_dir}/manifest" "${table_dir}/bucket-0"
    echo "INSIDE_TABLE" > "${table_dir}/bucket-0/data.txt"
    echo '{"version":3,"id":0,"highestFieldId":0,"partitionKeys":[],"primaryKeys":[],"options":{},"timeMillis":0,"fields":[{"id":0,"name":"data","type":"STRING NOT NULL"}]}' > "${table_dir}/schema/schema-0"
    echo -n '1' > "${table_dir}/snapshot/LATEST"
    echo '{"id":1,"schemaId":0,"baseManifestList":"manifest-list-1","deltaManifestList":"manifest-list-1","commitUser":"test","commitIdentifier":0,"commitKind":"APPEND","timeMillis":0}' > "${table_dir}/snapshot/snapshot-1"

    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO FUNCTION file('${table_dir}/manifest/manifest-list-1', 'Avro',
            '_FILE_NAME String, _FILE_SIZE Int64, _NUM_ADDED_FILES Int64, _NUM_DELETED_FILES Int64,
             _PARTITION_STATS Tuple(_MAX_VALUES String, _MIN_VALUES String, _NULL_COUNTS Array(Int64)), _SCHEMA_ID Int64')
        VALUES ('$2', 1, 1, 0, ('', '', [0]), 0)"

    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO FUNCTION file('${table_dir}/manifest/manifest-1', 'Avro',
            '_KIND Int32, _PARTITION String, _BUCKET Int32, _TOTAL_BUCKETS Int32,
             _FILE Tuple(_FILE_NAME String, _FILE_SIZE Int64, _ROW_COUNT Int64, _MIN_KEY String, _MAX_KEY String,
                 _KEY_STATS Tuple(_MAX_VALUES String, _MIN_VALUES String, _NULL_COUNTS Array(Int64)),
                 _VALUE_STATS Tuple(_MAX_VALUES String, _MIN_VALUES String, _NULL_COUNTS Array(Int64)),
                 _MIN_SEQUENCE_NUMBER Int64, _MAX_SEQUENCE_NUMBER Int64, _SCHEMA_ID Int64, _LEVEL Int32,
                 _EXTRA_FILES Array(String), _CREATION_TIME Nullable(DateTime64(6)), _DELETE_ROW_COUNT Nullable(Int64),
                 _EMBEDDED_FILE_INDEX Nullable(String), _FILE_SOURCE Nullable(Int8), _VALUE_STATS_COLS Array(Int64))')
        VALUES (0, unhex('00000000'), 0, 1,
            ('$3', 13, 1, '', '', ('', '', [0]), ('', '', [0]), 0, 0, 0, 0, [], NULL, NULL, NULL, NULL, []))"
}

read_table()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT data FROM paimonLocal('${BASE_DIR}/$1', 'RawBLOB', 'data String')" 2>&1 \
        | grep -o -m1 -E "INSIDE_TABLE|SECRET_OUTSIDE_TABLE|PATH_ACCESS_DENIED"
}

create_table valid 'manifest-1' 'data.txt'
read_table valid

create_table data_file_dotdot 'manifest-1' '../../outside/secret.txt'
read_table data_file_dotdot

create_table data_file_absolute 'manifest-1' "${BASE_DIR}/outside/secret.txt"
read_table data_file_absolute

create_table manifest_dotdot '../../valid/manifest/manifest-1' 'data.txt'
read_table manifest_dotdot

create_table manifest_absolute "${BASE_DIR}/valid/manifest/manifest-1" 'data.txt'
read_table manifest_absolute

rm -rf "${BASE_DIR}"
