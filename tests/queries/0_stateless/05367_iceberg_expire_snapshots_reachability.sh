#!/usr/bin/env bash
# `expire_snapshots` must leave the current state intact: deleted rows stay deleted, answers computed
# from file statistics match a full read, time travel to an expired snapshot fails loudly, another
# table over the same path never returns missing rows, and files whose deletion was lost are
# collected by `remove_orphan_files`.
#
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --session_timezone=UTC --allow_insert_into_iceberg=1 --allow_experimental_expire_snapshots=1 --allow_iceberg_remove_orphan_files=1"

TABLE="t_${CLICKHOUSE_DATABASE}"
STALE="${TABLE}_stale"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}"

list_files() { (cd "${TABLE_PATH}" && find . -type f | sort); }

$CH --query "
    CREATE TABLE ${TABLE} (id Int64, p Int64)
    ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet')
    PARTITION BY p
    SETTINGS iceberg_format_version = 2, allow_experimental_iceberg_compaction = 0"
$CH --query "INSERT INTO ${TABLE} VALUES (1, 1), (2, 2), (3, 3)"
$CH --query "INSERT INTO ${TABLE} VALUES (4, 1)"
$CH --mutations_sync=2 --query "ALTER TABLE ${TABLE} DELETE WHERE id = 2"

read -r first_snapshot first_timestamp_ms <<< "$($CH --query "
    SELECT snapshot_id, toUnixTimestamp64Milli(made_current_at) FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}' ORDER BY made_current_at LIMIT 1")"

# A second table over the same files plays a replica that loaded the metadata before the maintenance.
$CH --query "CREATE TABLE ${STALE} ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet')"
$CH --query "SELECT count() FROM ${STALE}" > /dev/null

# After this, the files of partition 3 belong only to the snapshots that are about to expire.
$CH --query "ALTER TABLE ${TABLE} DROP PARTITION 3"
expected=$($CH --query "SELECT arraySort(groupArray(id)) FROM ${TABLE}")
echo "current ${expected}"

files_before=$(list_files)
cp -Rp "${TABLE_PATH}" "${TABLE_PATH}.saved"

echo "expire"
$CH --query "ALTER TABLE ${TABLE} EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1)" | grep -v $'\t0$'
$CH --query "SELECT count() FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}'"
$CH --query "SELECT arraySort(groupArray(id)) FROM ${TABLE}"
for use_file_stats in 1 0; do
    $CH --query "SELECT count(), min(id), max(id) FROM ${TABLE}
        SETTINGS optimize_count_from_files = ${use_file_stats}, optimize_min_max_from_files = ${use_file_stats}"
done

echo "time travel to the expired snapshot"
$CH --query "SELECT count() FROM ${TABLE} SETTINGS iceberg_snapshot_id = ${first_snapshot}" 2>&1 | grep -o -m1 'No snapshot found'
$CH --query "SELECT count() FROM ${TABLE} SETTINGS iceberg_timestamp_ms = ${first_timestamp_ms}" 2>&1 | grep -o -m1 'No snapshot found'

# The stale table may fail, but it must never return fewer rows than exist.
stale=$($CH --query "SELECT arraySort(groupArray(id)) FROM ${STALE}" 2>&1)
if [ "${stale}" = "${expected}" ] || [[ "${stale}" == *"Code: "* ]]; then
    echo "stale table: current data or an error"
else
    echo "stale table returned ${stale}"
fi

echo "deletes lost after the commit are collected as orphans"
files_after=$(list_files)
for file in $(comm -23 <(echo "${files_before}") <(echo "${files_after}")); do
    cp -p "${TABLE_PATH}.saved/${file}" "${TABLE_PATH}/${file}"
done
$CH --query "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files('$($CH --query "SELECT toString(now())")')" | grep -v $'\t0$' | grep -v metadata_files
[ "$(list_files | grep -v '/v1.metadata.json')" = "$(echo "${files_after}" | grep -v '/v1.metadata.json')" ] && echo "same files as right after expire"
$CH --query "SELECT arraySort(groupArray(id)) FROM ${TABLE}"

$CH --query "DROP TABLE ${STALE}; DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}" "${TABLE_PATH}.saved"

echo "scale"
TABLE="${TABLE}_scale"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}"
$CH --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet') SETTINGS allow_experimental_iceberg_compaction = 0"
$CH --query "$(for i in $(seq 100); do echo "INSERT INTO ${TABLE} VALUES (${i});"; done)"
$CH --query "ALTER TABLE ${TABLE} EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1)" | grep -v $'\t0$'
$CH --query "SELECT count() FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}'"
$CH --query "SELECT count(), sum(id) FROM ${TABLE}"
$CH --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
