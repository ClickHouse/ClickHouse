#!/usr/bin/env bash
# Tags: no-fasttest
# A data file referenced by the metadata but absent from local object storage used to
# surface as a raw `std::filesystem::filesystem_error` (`STD_EXCEPTION`, code 1001),
# which is not catchable by error code. It must be a `FILE_DOESNT_EXIST` instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_DIR="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_paimon_missing_data_file"

rm -rf "${TABLE_DIR}"
mkdir -p "${USER_FILES_PATH}"
cp -r "${CUR_DIR}/data_minio/paimon_no_partition" "${TABLE_DIR}"

# Sanity check: the untouched copy reads back. `FORMAT Null` still reads every data
# file, so neither query can be answered from the manifest row counts alone.
${CLICKHOUSE_CLIENT} --enable_time_time64_type=1 -q "SELECT * FROM paimonLocal('${TABLE_DIR}') FORMAT Null" && echo "OK"

# Drop the single data file the manifest points at, keeping all metadata intact.
rm -f "${TABLE_DIR}"/bucket-0/*.parquet

${CLICKHOUSE_CLIENT} --enable_time_time64_type=1 -q "SELECT * FROM paimonLocal('${TABLE_DIR}') FORMAT Null" 2>&1 | grep -c -F "FILE_DOESNT_EXIST"

rm -rf "${TABLE_DIR}"
