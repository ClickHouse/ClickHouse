#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

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
mkdir -p "${BASE_DIR}/rel"
(
    cd "${BASE_DIR}" || exit 1
    ${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 -q "
    CREATE TABLE ice_rel (id UInt64) ENGINE = IcebergLocal('rel/');
    INSERT INTO ice_rel SELECT number FROM numbers(5);
    SELECT DISTINCT startsWith(_path, '/') FROM ice_rel;
    "
)
