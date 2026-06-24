#!/usr/bin/env bash
# Tags: no-fasttest
#
# Regression test: `ALTER TABLE ... DROP PARTITION` on a partition that already
# has position-delete files (left behind by a prior `DELETE FROM`) must not
# corrupt the snapshot summary `total-files-size`.
#
# `DELETE FROM` writes the position-delete file but used to record
# `added-files-size = 0`, while `DROP PARTITION` removing that delete file does
# count its size in `removed-files-size`. The size was therefore subtracted
# without ever being added, so the unsigned `total-files-size` underflowed and
# was serialized as a negative number (e.g. `-924`). Reading it back through
# `SnapshotSummary::fromJSON` (e.g. via `system.iceberg_history`) then threw
# `CANNOT_PARSE_NUMBER: Unsigned type must not contain '-' symbol`, so the whole
# table was reported as broken and produced no history rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_drop_after_delete"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"

# Partition 1 gets three rows; partition 2 one row.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'x'), (1, 'y'), (1, 'z'), (2, 'keep')"
# Position delete inside partition 1 (writes a delete file).
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE a = 1 AND b = 'y'"
# Drop the whole partition, removing both its data file and its delete file.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1"

# The history must be readable (no "broken table"): we expect exactly the three
# commits APPEND / OVERWRITE / DELETE. Before the fix this returned nothing
# because the negative `total-files-size` made the summary unparseable.
echo "--- operation counts (expect APPEND 1, OVERWRITE 1, DELETE 1) ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT operation, count()
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    GROUP BY operation
    ORDER BY operation
    FORMAT TSV
"

# No snapshot may carry a negative (underflowed) total-files-size.
echo "--- snapshots with negative total-files-size (expect 0) ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT countIf(toInt64(summary['total-files-size']) < 0)
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    FORMAT TSV
"

# Only partition 2 ('keep') survives.
echo "--- surviving rows (expect 1) ---"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} FORMAT TSV"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
