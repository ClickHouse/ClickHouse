#!/usr/bin/env bash
# `remove_orphan_files` and `expire_snapshots` read each manifest once, not once per snapshot that lists
# it, and still collect every file it references.
#
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-replicated-database: `ALTER TABLE ... EXECUTE` runs as replicated DDL there and returns no result rows

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --allow_iceberg_remove_orphan_files=1 --allow_experimental_expire_snapshots=1"

# Ten single-row `INSERT`s: snapshot k lists the k manifests written so far.
create_table()
{
    $CH --query "CREATE TABLE $1 (id Int64) ENGINE = IcebergLocal('$2/', 'Parquet') SETTINGS allow_experimental_iceberg_compaction = 0"
    $CH --query "$(for i in $(seq 10); do echo "INSERT INTO $1 VALUES ($i);"; done)"
}

# Prints the non-zero metrics of the command, then the number of repeated manifest reads and of
# distinct manifests read, per execution of the command.
execute_logged()
{
    $CH --iceberg_metadata_log_level manifest_file_metadata --query "ALTER TABLE $1 EXECUTE $2" | grep -v $'\t0$'
    $CH --query "
        SYSTEM FLUSH LOGS iceberg_metadata_log;
        SELECT max(reads - manifests), max(manifests) FROM (
            SELECT query_id, count() AS reads, uniqExact(file_path) AS manifests
            FROM system.iceberg_metadata_log
            WHERE content_type = 'ManifestFileMetadata' AND position(table_path, '$1') > 0
            GROUP BY query_id)"
}

echo "remove_orphan_files"
TABLE="t_orphan_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH:?}/${TABLE}"
create_table "${TABLE}" "${TABLE_PATH}"
# Every file passes the age filter, so a reachable file that is not collected is counted as an orphan.
echo "junk" > "${TABLE_PATH}/data/orphan.parquet"
find "${TABLE_PATH}" -type f -exec env TZ=UTC touch -t 202001010000 {} +
# The writer leaves `v1.metadata.json` out of `metadata-log`, so it is counted as well.
execute_logged "${TABLE}" "remove_orphan_files(older_than = '2023-01-01 00:00:00', dry_run = 1)"
$CH --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"

echo "expire_snapshots"
TABLE="t_expire_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH:?}/${TABLE}"
create_table "${TABLE}" "${TABLE_PATH}"
ROOT=$($CH --query "
    SELECT snapshot_id FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
        AND parent_id NOT IN (SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}')")
# The other nine snapshots still reference the manifest and the data file of the first one.
execute_logged "${TABLE}" "expire_snapshots(snapshot_ids = [${ROOT}], dry_run = 1)"
$CH --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
