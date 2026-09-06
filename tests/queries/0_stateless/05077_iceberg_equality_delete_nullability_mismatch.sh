#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: IcebergLocal is non-replicated.

# The table has a required `data` column, while the equality delete file declares the same column
# as optional. The set for the `notIn` filter used to be built with the table-schema type and then
# filled from the delete file columns of a different type (a column type confusion), which crashed
# the server in `ColumnString::insertRangeFrom`.
# The fixture is described in data_minio/deletes_db/README.md (`eq_deletes_required_table`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAKE_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/eq_deletes_required_table"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
cp -r "${CUR_DIR}/data_minio/deletes_db/eq_deletes_required_table" "${LAKE_DIR}"

${CLICKHOUSE_CLIENT} --query "SELECT data FROM icebergLocal('${LAKE_DIR}/') ORDER BY data"
${CLICKHOUSE_CLIENT} --query "SELECT id, data FROM icebergLocal('${LAKE_DIR}/') ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${LAKE_DIR}/')"

rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
