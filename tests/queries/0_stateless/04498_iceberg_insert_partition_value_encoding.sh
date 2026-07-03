#!/usr/bin/env bash
# Tags: no-fasttest
#
# Regression test: manifest partition values must be encoded with the Avro type derived
# from the partition column type, not from the `Field` tag. `UInt8/16/32` values are
# stored as `Field::UInt64`, while their manifest partition field is declared Avro `int`,
# so keying on the tag emitted a `long` datum for an `int` field and the INSERT failed.
# `DateTime64` values are stored as `Field::Decimal64` and must be unwrapped to the
# underlying ticks.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_partition_encoding"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

echo "--- UInt32 partition (Avro int) ---"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a UInt32, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'x'), (2, 'y'), (3, 'z')"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "SELECT b FROM ${TABLE} WHERE a = 2 FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"

echo "--- DateTime64 partition (Avro long from Decimal64 ticks) ---"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (t DateTime64(6, 'UTC'), v String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (t)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} VALUES ('2024-01-01 00:00:00.123456', 'a'), ('2024-06-15 12:30:00.654321', 'b')
"
${CLICKHOUSE_CLIENT} --query "SELECT t, v FROM ${TABLE} ORDER BY t FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
