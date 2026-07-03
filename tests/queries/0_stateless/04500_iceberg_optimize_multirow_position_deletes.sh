#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Regression test: the position-delete-only OVERWRITE classifier in compaction compared
# a row count (`added-position-deletes`) with a file count (`added-delete-files`), so a
# normal position-delete `DELETE FROM` writing one delete file with several rows made a
# later `OPTIMIZE TABLE` throw `NOT_IMPLEMENTED`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_multirow_deletes"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id Int32, v String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')
"

# One DELETE mutation removing several rows: one position-delete file, multiple delete rows.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE id IN (2, 4)"

echo "--- count after DELETE ---"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} FORMAT TSV"

${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}"

echo "--- rows after OPTIMIZE ---"
${CLICKHOUSE_CLIENT} --query "SELECT id, v FROM ${TABLE} ORDER BY id FORMAT TSV"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
