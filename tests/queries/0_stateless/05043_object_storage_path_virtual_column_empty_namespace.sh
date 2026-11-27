#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option) and Azurite

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# A server warning would land on stderr, which clickhouse-test turns into a failure.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL="none"
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap "rm -rf '${BASE_DIR}' 2>/dev/null" EXIT
rm -rf "${BASE_DIR}"
mkdir -p "${BASE_DIR}/abs"

CLIENT="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1"

$CLIENT -q "
CREATE TABLE ice (id UInt64) ENGINE = IcebergLocal('${BASE_DIR}/abs/');
INSERT INTO ice SELECT number FROM numbers(10);
"

# `_path` names the file, so for an absolute engine argument it is an absolute path.
$CLIENT -q "SELECT DISTINCT startsWith(_path, '${BASE_DIR}/abs/') FROM ice;"

# The two columns must not disagree about the directory holding the same file.
$CLIENT -q "
SELECT DISTINCT
    arrayStringConcat(arrayPopBack(splitByChar('/', _path)), '/')
        = arrayStringConcat(arrayPopBack(splitByChar('/', _iceberg_metadata_file_path)), '/')
FROM ice;
"

# A filter on `_path` is evaluated by a listing prefilter, not only by the column, so this
# arm reaches a second formatter and returns 0 unless both agree with each other.
$CLIENT -q "SELECT count() FROM ice WHERE _path IN (SELECT DISTINCT _path FROM ice);"

# A relative engine argument stays relative: the leading separator is what distinguishes an
# absolute path from a relative one, so it must not be invented for a path that lacks it.
# This arm passes before and after the fix; it fails if the empty-prefix case prepends '/'.
# It runs under clickhouse-local, whose path prefix is the root, because a server resolves a
# relative argument against user_files_path and rejects it before the read.
# The same join is also performed by the non-glob listing prefilter, which reaches it with a
# namespace rather than without one. A key that keeps a leading separator must land under the
# namespace there too, or the prefilter drops the only file before it is read. Azure is the
# reachable backend: S3 rejects such a key in `S3::URI::validateKey`.
AZURE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
# Azure container names are limited to 63 characters, so hash the unique name instead of
# embedding it, and keep it lowercase alphanumeric as the naming rules require.
AZURE_CONT="cont$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | md5sum | cut -c1-24)"
AZURE_ARGS="'${AZURE_CONN}', '${AZURE_CONT}', '/slashed.csv', 'CSV', 'auto', 'x UInt64'"

$CLIENT -q "
INSERT INTO FUNCTION azureBlobStorage(${AZURE_ARGS}) SELECT 1 AS x SETTINGS azure_truncate_on_insert = 1;
" > /dev/null

# Compared against itself, so it pins the two spellings agreeing rather than either literal.
$CLIENT -q "
SELECT count() FROM azureBlobStorage(${AZURE_ARGS})
WHERE _path IN (SELECT _path FROM azureBlobStorage(${AZURE_ARGS}));
"

mkdir -p "${BASE_DIR}/rel"
(
    cd "${BASE_DIR}" || exit 1
    ${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 -q "
    CREATE TABLE ice_rel (id UInt64) ENGINE = IcebergLocal('rel/');
    INSERT INTO ice_rel SELECT number FROM numbers(5);
    SELECT DISTINCT startsWith(_path, '/') FROM ice_rel;
    "
)
