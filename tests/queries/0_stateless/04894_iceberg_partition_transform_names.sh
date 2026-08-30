#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/114848:
# partition spec must publish canonical Iceberg transform names `day`/`hour`,
# not the non-standard plural forms `days`/`hours`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DAY_TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_day"
DAY_PATH="${USER_FILES_PATH}/${DAY_TABLE}/"
HOUR_TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_hour"
HOUR_PATH="${USER_FILES_PATH}/${HOUR_TABLE}/"

trap 'rm -rf "${DAY_PATH}" "${HOUR_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${DAY_TABLE} (k Int64, ts DateTime)
    ENGINE = IcebergLocal('${DAY_PATH}', 'Parquet')
    PARTITION BY (toRelativeDayNum(ts))
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${DAY_TABLE}
    SELECT number, toDateTime('2026-01-01 00:00:00') + number * 3600 FROM numbers(10)
"

grep -ho '"transform" : "[^"]*"' "${DAY_PATH}metadata/"*.json | sort -u

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${HOUR_TABLE} (k Int64, ts DateTime)
    ENGINE = IcebergLocal('${HOUR_PATH}', 'Parquet')
    PARTITION BY (toRelativeHourNum(ts))
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${HOUR_TABLE}
    SELECT number, toDateTime('2026-01-01 00:00:00') + number * 3600 FROM numbers(10)
"

grep -ho '"transform" : "[^"]*"' "${HOUR_PATH}metadata/"*.json | sort -u
