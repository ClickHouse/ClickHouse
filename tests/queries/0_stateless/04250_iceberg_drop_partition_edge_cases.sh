#!/usr/bin/env bash
# Tags: no-fasttest
#
# Edge cases for `ALTER TABLE ... DROP PARTITION` on Iceberg:
#   * Time-travel via `iceberg_snapshot_id` after DROP -- the parent snapshot
#     must still see the dropped rows, which exercises the EXISTING-status
#     preservation of original snapshot_id / sequence_number when survivors
#     are re-emitted, plus the fact that the parent's manifest list is left
#     intact (we only modify the new snapshot's manifest list).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_tt"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

echo "=== Time-travel after DROP ==="

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'drop-me-1'), (1, 'drop-me-2'), (2, 'keep')"

# Snapshot id BEFORE the DROP -- this is the parent we want to time-travel back to.
SNAPSHOT_BEFORE_DROP=$(${CLICKHOUSE_CLIENT} --query "SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}' ORDER BY made_current_at DESC LIMIT 1")

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1"

echo "--- current snapshot (after DROP) ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a, b"

echo "--- parent snapshot (time-travel) ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM ${TABLE} ORDER BY a, b SETTINGS iceberg_snapshot_id = ${SNAPSHOT_BEFORE_DROP}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"

# Decimal partition columns are currently rejected on CREATE TABLE by the
# Iceberg writer ("Unsupported type for iceberg Decimal(...)") -- separate
# from DROP PARTITION. The Decimal32/Decimal64 cases in `writePartitionRecord`
# are reachable only via Spark-written tables today. When Decimal partition
# columns become writable, uncomment to verify DROP PARTITION matches by
# Decimal value.
#
# TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_dec"
# TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
# ${CLICKHOUSE_CLIENT} --query "
#     CREATE TABLE ${TABLE} (id Int64, d Decimal32(2))
#     ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
#     PARTITION BY (d)
# "
# ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 1.50), (2, 1.50), (3, 2.75)"
# ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1.50"
# ${CLICKHOUSE_CLIENT} --query "SELECT id, d FROM ${TABLE} ORDER BY id"
# ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
# rm -rf "${TABLE_PATH}"
