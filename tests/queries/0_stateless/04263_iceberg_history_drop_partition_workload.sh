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

# The current head snapshot is the unique current ancestor that is not the parent
# of any other current ancestor (the leaf of the linear commit chain). We identify
# it structurally rather than by `made_current_at`, whose millisecond resolution can
# tie for back-to-back commits and make ordering non-deterministic.
HEAD_SNAPSHOT_PREDICATE="
    is_current_ancestor
    AND snapshot_id NOT IN (
        SELECT parent_id FROM system.iceberg_history
        WHERE database = currentDatabase() AND table = '${TABLE}'
          AND is_current_ancestor AND parent_id != 0
    )
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
          AND ${HEAD_SNAPSHOT_PREDICATE}
        FORMAT TSV
    "
}

# Step 1: INSERT spanning several partitions in a single statement.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} VALUES (1, 'a'), (1, 'b'), (2, 'c'), (3, 'd')
"
latest_snapshot "step 1: multi-partition INSERT"

# Step 2: four INSERTs, each into a distinct new partition (a = 100..103). Each is a
# separate snapshot; they are sent on a single client connection to avoid per-statement
# process startup (the metadata rescans, not the row count, dominate this test's runtime).
STEP2_INSERTS=""
for i in $(seq 100 103); do
    STEP2_INSERTS+="INSERT INTO ${TABLE} VALUES (${i}, 'p${i}');"
done
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "${STEP2_INSERTS}"
latest_snapshot "step 2: 4 single-partition INSERTs (last)"

# Sanity: 8 records across 7 distinct partitions (a = 1 holds two rows), 7 data files.
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
          AND ${HEAD_SNAPSHOT_PREDICATE}
    ) AS s
    SELECT count() AS rows, uniqExact(a) AS parts, s.1 AS files_in_summary, s.2 AS rows_in_summary,
           rows = s.2 AS rows_match
    FROM ${TABLE}
    FORMAT TSV
"

# Operation counts across the whole history. We assert per-operation counts rather
# than the ordered sequence, because `made_current_at` ties (see above) make the row
# order non-deterministic; the counts do not depend on order.
echo "--- operation counts ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT operation, count()
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    GROUP BY operation
    ORDER BY operation
    FORMAT TSV
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
