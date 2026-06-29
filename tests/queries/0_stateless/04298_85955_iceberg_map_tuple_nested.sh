#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/85955
# Reading an Iceberg table with a Map(Tuple(Int), Tuple(Int)) column used to
# throw std::out_of_range from readNonNullableColumnFromArrowColumn during
# Parquet column-name remapping for the nested tuple key/value paths.
# The current native v3 Parquet reader path handles the nested-tuple case
# correctly, so insert + select on such a table must return cleanly.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Map(Tuple(Int), Tuple(Int)))
    ENGINE = IcebergLocal('${TABLE_PATH}')
"

# Use the same generateRandom seed reported in the original issue.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO TABLE ${TABLE} (c0)
    SELECT c0
    FROM generateRandom('c0 Map(Tuple(Int), Tuple(Int))', 903856812385940645, 1, 1)
    LIMIT 1
"

# Force the nested column read path. `SELECT count()` alone can be answered
# from Parquet row-group metadata via `optimize_count_from_files` and would
# not exercise `readNonNullableColumnFromArrowColumn` where the original
# `std::out_of_range` was raised.
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE} FORMAT Null"

# Sanity check on the row we inserted.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
