#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"

# Regression test for #121514: an Iceberg sort-order field naming a transform that
# ClickHouse cannot parse used to dereference a disengaged std::optional in
# getSortingKeyDescriptionFromMetadata and abort the whole server process.
# It must surface as a query error instead.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (x Int64, y String)
        ENGINE = IcebergLocal('${TABLE_PATH}')
        ORDER BY icebergTruncate(3, x)
        SETTINGS iceberg_format_version = 2;
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (10, 'a'), (20, 'b')"

# Rewrite the transform in the latest metadata file to a value that
# parseTransformAndArgument cannot handle ('bucket' with a non-numeric
# argument returns std::nullopt).
LATEST=$(ls "${TABLE_PATH}metadata"/v*.metadata.json | sort -V | tail -1)
sed -i 's/"truncate\[3\]"/"bucket[xyz]"/' "${LATEST}"

# Reading the table must raise BAD_ARGUMENTS, not kill the server.
${CLICKHOUSE_CLIENT} --query "SELECT * FROM icebergLocal('${TABLE_PATH}', 'Parquet')" 2>&1 | grep -m1 -c BAD_ARGUMENTS

# The server must still be alive.
${CLICKHOUSE_CLIENT} --query "SELECT 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
