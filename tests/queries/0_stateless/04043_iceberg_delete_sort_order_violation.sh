#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/100034
# ALTER TABLE DELETE on an Iceberg table with multiple data files
# should not cause a "Sort order of blocks violated" exception.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 UUID)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    ORDER BY (c0)
"

# Two separate inserts create two data files with unsorted UUIDs across them.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES ('effc4717-f4dc-03c1-9b43-2a4057179197')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES ('a7253f79-da8a-ab73-ddc0-0b18adad08d8')"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE TRUE"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
