#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Regression test: the metadata rebuild of `OPTIMIZE TABLE` attributed every data file
# to every snapshot whose manifest list referenced its manifest (a manifest is carried
# forward by all later snapshots), so `total-records` of the rebuilt snapshots was
# inflated and a trivial `SELECT count()` returned a wrong result after OPTIMIZE.
# The rebuilt snapshots must also chain to the previously generated snapshot instead
# of the (possibly skipped) original parent.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_optimize_totals"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id Int32, v String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

# Three snapshots; each later manifest list also references the earlier manifests.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'a'), (2, 'b')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (3, 'c'), (4, 'd')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (5, 'e'), (6, 'f')"

# Add an overwrite snapshot between the retained append snapshots. The metadata rebuild skips
# overwrite snapshots, but must apply their position deletes before rebuilding the append history.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query \
    "ALTER TABLE ${TABLE} DELETE WHERE id = 2"

echo "--- count before OPTIMIZE ---"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} FORMAT TSV"

echo "--- delete statistics before OPTIMIZE ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT summary['total-delete-files'], summary['total-position-deletes'], summary['total-equality-deletes']
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    ORDER BY made_current_at DESC, snapshot_id DESC
    LIMIT 1
    FORMAT TSV
"

${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}"

echo "--- count after OPTIMIZE ---"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} FORMAT TSV"

echo "--- rows after OPTIMIZE ---"
${CLICKHOUSE_CLIENT} --query "SELECT id, v FROM ${TABLE} ORDER BY id FORMAT TSV"

echo "--- totals of the current snapshot ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT
        summary['total-records'],
        summary['total-data-files'],
        summary['total-delete-files'],
        summary['total-position-deletes'],
        summary['total-equality-deletes']
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    ORDER BY made_current_at DESC, snapshot_id DESC
    LIMIT 1
    FORMAT TSV
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
