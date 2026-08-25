#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/102508
# `ALTER TABLE ... DELETE` / `UPDATE` on Iceberg tables whose data file format
# is not Parquet must not crash the server (`LOGICAL_ERROR` in ORC, abort in Avro)
# or silently corrupt the table. Both are rejected with `NOT_IMPLEMENTED`, and
# Parquet tables continue to work.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ORC_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_orc"
AVRO_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_avro"
PARQUET_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_parquet"

rm -rf "${ORC_PATH}" "${AVRO_PATH}" "${PARQUET_PATH}"

# --- ORC: DELETE and UPDATE must be rejected ---
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg=1 --query "
    CREATE TABLE t_orc (id Int32, val String) ENGINE = IcebergLocal('${ORC_PATH}', 'ORC') SETTINGS iceberg_format_version = 2;
    INSERT INTO t_orc VALUES (1, 'a'), (2, 'b'), (3, 'c');
"

${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg=1 --query "ALTER TABLE t_orc DELETE WHERE id = 2" 2>&1 | grep -oE 'Code: [0-9]+|NOT_IMPLEMENTED' | head -1
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg=1 --query "ALTER TABLE t_orc UPDATE val = 'x' WHERE id = 1" 2>&1 | grep -oE 'Code: [0-9]+|NOT_IMPLEMENTED' | head -1

# Table must remain readable (no corruption).
${CLICKHOUSE_CLIENT} --query "SELECT id, val FROM t_orc ORDER BY id"

# --- Avro: DELETE and UPDATE must be rejected ---
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg=1 --query "
    CREATE TABLE t_avro (id Int32, val String) ENGINE = IcebergLocal('${AVRO_PATH}', 'Avro') SETTINGS iceberg_format_version = 2;
    INSERT INTO t_avro VALUES (1, 'a'), (2, 'b'), (3, 'c');
"

${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg=1 --query "ALTER TABLE t_avro DELETE WHERE id = 2" 2>&1 | grep -oE 'Code: [0-9]+|NOT_IMPLEMENTED' | head -1
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg=1 --query "ALTER TABLE t_avro UPDATE val = 'x' WHERE id = 1" 2>&1 | grep -oE 'Code: [0-9]+|NOT_IMPLEMENTED' | head -1

# Table must remain readable (no corruption).
${CLICKHOUSE_CLIENT} --query "SELECT id, val FROM t_avro ORDER BY id"

# --- Parquet: DELETE keeps working ---
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg=1 --query "
    CREATE TABLE t_parquet (id Int32, val String) ENGINE = IcebergLocal('${PARQUET_PATH}', 'Parquet') SETTINGS iceberg_format_version = 2;
    INSERT INTO t_parquet VALUES (1, 'a'), (2, 'b'), (3, 'c');
"

${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg=1 --query "ALTER TABLE t_parquet DELETE WHERE id = 2"
${CLICKHOUSE_CLIENT} --query "SELECT id, val FROM t_parquet ORDER BY id"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_orc"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_avro"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_parquet"
rm -rf "${ORC_PATH}" "${AVRO_PATH}" "${PARQUET_PATH}"
