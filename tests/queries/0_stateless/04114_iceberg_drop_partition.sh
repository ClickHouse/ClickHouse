#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'x'), (1, 'y'), (2, 'z'), (3, 'w')"

echo "=== Scenario 1: basic drop ==="
echo "--- before drop ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a, b"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 2"

echo "--- after drop partition 2 ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a, b"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 99"

echo "--- after drop partition 99 (no-op) ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a, b"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 3"

echo "--- after dropping all partitions ---"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION ALL" 2>&1 | grep -o 'NOT_IMPLEMENTED' | head -1

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_s2"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (Key Int64, Value String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (Value)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')"

echo "=== Scenario 2: multi-partition drop, then drop again, counts stay sane ==="
echo "--- before any drop ---"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 'a'"
echo "--- after drop partition 'a' ---"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --query "SELECT Key, Value FROM ${TABLE} ORDER BY Key"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 'b'"
echo "--- after drop partition 'b' ---"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_s3"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'x'), (1, 'y')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'z'), (2, 'p')"

echo "=== Scenario 3: multiple files per partition ==="
echo "--- before drop ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a, b"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1"
echo "--- after drop partition 1 ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a, b"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_s4"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'old'), (2, 'keep')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'new')"

echo "=== Scenario 4: drop then re-insert ==="
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a, b"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
