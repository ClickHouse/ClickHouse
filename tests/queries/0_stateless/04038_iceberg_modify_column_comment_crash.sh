#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_iceberg_mod_col"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}')
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1)"

# MODIFY COLUMN with only a COMMENT (no type change) has data_type=nullptr.
# This bypasses the to_remove guard in checkAlterIsPossible and reaches
# generateModifyColumnMetadata, which dereferences the null pointer.
# Expected: NOT_IMPLEMENTED error (not a server crash).
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    ALTER TABLE ${TABLE} MODIFY COLUMN c0 COMMENT 'test_comment'
" 2>&1 | grep -o "NOT_IMPLEMENTED"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
