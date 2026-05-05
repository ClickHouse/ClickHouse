#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

###############################################################################
# Scenario 1: basic single-column partition, drop one value
###############################################################################
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

# Drop non-existing partition: no-op
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 99"

echo "--- after drop partition 99 (no-op) ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a, b"

# Drop the last remaining partitions
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 3"

echo "--- after dropping all partitions ---"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

# DROP PARTITION ALL should be rejected
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION ALL" 2>&1 | grep -o 'NOT_IMPLEMENTED' | head -1

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"

###############################################################################
# Scenario 2: multi-partition INSERT, drop one; per-file stats are correct so
# subsequent counts are stable (regression test for the per-file record_count
# bug where each manifest entry inherited the snapshot-aggregate total).
###############################################################################
TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_s2"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
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

###############################################################################
# Scenario 3: multiple files per partition (achieved via two inserts into the
# same partition value), then drop that partition — exercises the rewrite path
# when surviving files sit alongside matched ones in the same manifest list.
###############################################################################
TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_s3"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
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

###############################################################################
# Scenario 4: DROP then re-INSERT into the same partition; the re-inserted rows
# must be visible and not shadowed by the prior drop.
###############################################################################
TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_s4"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
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

###############################################################################
# Scenario 5: DROP after DELETE FROM left position-delete files behind. The
# dangling position-deletes for the dropped partition must also be removed
# from the new snapshot's manifest list.
###############################################################################
TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_s5"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'x'), (1, 'y'), (1, 'z'), (2, 'keep')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE a = 1 AND b = 'y'"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1"

echo "=== Scenario 5: drop after DELETE FROM (position-deletes present) ==="
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a, b"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
