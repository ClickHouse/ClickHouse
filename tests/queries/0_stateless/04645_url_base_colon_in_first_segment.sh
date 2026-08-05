#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3 (minio)

# A relative name whose first path segment contains a colon (e.g. `report:2026.csv`) technically
# parses as an absolute URI with the scheme `report`, but it is not a usable URL, so `url_base`,
# `s3_base` and the base URL of the `URL` database must resolve it as a relative path.
# https://github.com/ClickHouse/ClickHouse/pull/111512

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo '--- url_base with a file:// base in clickhouse-local'
DATA_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DATA_DIR}"
printf '1\n2\n' > "${DATA_DIR}/report:2026.csv"
(cd "${DATA_DIR}" && ${CLICKHOUSE_LOCAL} -q "SELECT * FROM url('report:2026.csv') ORDER BY ALL SETTINGS url_base = 'file://'")

echo '--- the default database of clickhouse-local'
(cd "${DATA_DIR}" && ${CLICKHOUSE_LOCAL} -q "SELECT count() FROM \`report:2026.csv\`")

echo '--- s3_base'
BUCKET_URL="http://localhost:11111/test"
FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}_report:2026.csv"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3('${BUCKET_URL}/${FILE}', 'test', 'testtest', 'CSV', 'n UInt32') SELECT number FROM numbers(3) SETTINGS s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM s3('${FILE}', 'test', 'testtest', 'CSV', 'n UInt32') SETTINGS s3_base = '${BUCKET_URL}/'"

rm -rf "${DATA_DIR}"
