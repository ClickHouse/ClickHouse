#!/usr/bin/env bash
# Tags: no-fasttest
#
# End-to-end workload test for `ALTER TABLE ... DROP PARTITION` on Iceberg:
# multi-partition INSERTs, ten single-partition INSERTs, a partial DROP, a
# follow-up INSERT, `OPTIMIZE` (compaction), and a final DROP. After each step
# we re-query `system.iceberg_history` and assert the latest snapshot's
# operation and the deterministic counts in its summary.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_workload"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"

# Helper: print the latest snapshot's operation and key summary counts.
# Selecting a deterministic subset of the map (counts only, no file sizes)
# keeps the reference stable across runs.
latest_snapshot() {
    local label="$1"
    echo "--- ${label} ---"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT
            operation,
            summary['added-data-files'],
            summary['added-records'],
            summary['removed-data-files'],
            summary['deleted-records'],
            summary['total-data-files'],
            summary['total-records']
        FROM system.iceberg_history
        WHERE database = currentDatabase() AND table = '${TABLE}'
        ORDER BY made_current_at DESC
        LIMIT 1
        FORMAT TSV
    "
}

# Step 1: INSERT spanning several partitions in a single statement.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} VALUES (1, 'a'), (1, 'b'), (2, 'c'), (3, 'd')
"
latest_snapshot "step 1: multi-partition INSERT"

# Step 2: ten INSERTs, each into a distinct new partition (a = 100..109).
for i in $(seq 100 109); do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (${i}, 'p${i}')"
done
latest_snapshot "step 2: 10 single-partition INSERTs (last)"

# Sanity: 14 distinct partitions total, 14 data files, 14 records.
echo "--- counts after step 2 ---"
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(a) FROM ${TABLE} FORMAT TSV"

# Step 3: drop two of the per-partition INSERTs and one row from the multi-INSERT.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 100"
latest_snapshot "step 3a: DROP PARTITION 100"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 101"
latest_snapshot "step 3b: DROP PARTITION 101"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1"
latest_snapshot "step 3c: DROP PARTITION 1 (drops 2 rows from multi-INSERT)"

# Step 4: another INSERT into a brand-new partition.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (200, 'after-drop')"
latest_snapshot "step 4: INSERT after DROP"

# Step 5: OPTIMIZE (compaction). ClickHouse writes an APPEND-flavored snapshot
# per source snapshot it rewrites, so the latest snapshot after OPTIMIZE is
# also an APPEND. The compaction does not change row totals.
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}"
echo "--- step 5: counts after OPTIMIZE (totals must be preserved) ---"
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(a) FROM ${TABLE} FORMAT TSV"

# Step 6: final DROP after compaction.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 200"
latest_snapshot "step 6: DROP PARTITION 200 (after OPTIMIZE)"

# Final invariant: rows and partitions visible to a normal SELECT match what
# the latest snapshot's `total-*` summary advertises.
echo "--- final SELECT vs total-* summary ---"
${CLICKHOUSE_CLIENT} --query "
    WITH (
        SELECT (toUInt64(summary['total-data-files']), toUInt64(summary['total-records']))
        FROM system.iceberg_history
        WHERE database = currentDatabase() AND table = '${TABLE}'
        ORDER BY made_current_at DESC LIMIT 1
    ) AS s
    SELECT count() AS rows, uniqExact(a) AS parts, s.1 AS files_in_summary, s.2 AS rows_in_summary,
           rows = s.2 AS rows_match
    FROM ${TABLE}
    FORMAT TSV
"

# Operation sequence across the whole history.
echo "--- operation sequence ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT operation
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    ORDER BY made_current_at
    FORMAT TSV
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
