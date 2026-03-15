#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test: ALTER TABLE ... MODIFY COLUMN ... COMMENT on an IcebergLocal
# table crashes the server with a null pointer dereference.
# When only a COMMENT is changed (no type), AlterCommand has data_type=nullptr.
# The guard in checkAlterIsPossible only rejects to_remove != NO_PROPERTY,
# so comment-only changes pass through to generateModifyColumnMetadata
# which dereferences the null pointer in MetadataGenerator.cpp.
# Expected: NOT_IMPLEMENTED error (not a server crash).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int)
    ENGINE = IcebergLocal('${TABLE_PATH}')
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1)"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    ALTER TABLE ${TABLE} MODIFY COLUMN c0 COMMENT 'test_comment'
" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
