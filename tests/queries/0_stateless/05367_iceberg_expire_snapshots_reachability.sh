#!/usr/bin/env bash
# `expire_snapshots` must leave the current state intact: deleted rows stay deleted, answers computed
# from file statistics match a full read, time travel to an expired snapshot fails loudly, another
# table over the same path never returns missing rows, and files whose deletion was lost are
# collected by `remove_orphan_files`.
#
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading format version 3 metadata logs a spurious "violates Iceberg specification" warning,
# https://github.com/ClickHouse/ClickHouse/issues/125333
CH="${CLICKHOUSE_CLIENT} --send_logs_level=error --session_timezone=UTC --allow_insert_into_iceberg=1 --allow_experimental_expire_snapshots=1 --allow_iceberg_remove_orphan_files=1 --iceberg_orphan_files_min_retention_seconds=0"

# Format version 3 has no DELETE or DROP PARTITION, so there it covers only the expire commit itself.
for format_version in 2 3; do
    echo "format version ${format_version}"
    TABLE="t_${CLICKHOUSE_DATABASE}_v${format_version}"
    STALE="${TABLE}_stale"
    TABLE_PATH="${USER_FILES_PATH}/${TABLE}"

    list_files() { (cd "${TABLE_PATH}" && find . -type f | sort); }

    $CH --query "
        CREATE TABLE ${TABLE} (id Int64, p Int64)
        ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet')
        PARTITION BY p
        SETTINGS iceberg_format_version = ${format_version}, allow_experimental_iceberg_compaction = 0"
    $CH --query "INSERT INTO ${TABLE} VALUES (1, 1), (2, 2), (3, 3)"
    $CH --query "INSERT INTO ${TABLE} VALUES (4, 1)"
    [ "${format_version}" = 2 ] && $CH --mutations_sync=2 --query "ALTER TABLE ${TABLE} DELETE WHERE id = 2"

    read -r first_snapshot first_timestamp_ms <<< "$($CH --query "
        SELECT snapshot_id, toUnixTimestamp64Milli(made_current_at) FROM system.iceberg_history
        WHERE database = currentDatabase() AND table = '${TABLE}' ORDER BY made_current_at LIMIT 1")"

    # A second table over the same files plays a replica that loaded the metadata before the maintenance.
    $CH --query "CREATE TABLE ${STALE} ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet')"
    $CH --query "SELECT count() FROM ${STALE}" > /dev/null

    # After this, the files of partition 3 belong only to the snapshots that are about to expire.
    # DROP PARTITION, like DELETE, is supported only for format version 2.
    [ "${format_version}" = 2 ] && $CH --query "ALTER TABLE ${TABLE} DROP PARTITION 3"
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
done

echo "scale"
# Without the metadata cache every read of a metadata file opens it, so `FileOpen` counts reads.
# Each snapshot adds one manifest list and one manifest: a linear pass opens about 2 files per snapshot,
# and the ceiling of 4 per snapshot still catches quadratic work (about 30 per snapshot here).
SCALE="${CH} --use_iceberg_metadata_files_cache=0 --async_insert=0"
TABLE="t_${CLICKHOUSE_DATABASE}_scale"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}"
$SCALE --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet') SETTINGS allow_experimental_iceberg_compaction = 0"
$SCALE --query "$(for i in $(seq 30); do echo "INSERT INTO ${TABLE} VALUES (${i});"; done)"
$SCALE --query_id "${CLICKHOUSE_DATABASE}_expire" --query "
    ALTER TABLE ${TABLE} EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1)" | grep -v $'\t0$'

# Runs after the expire, with one snapshot left: with many snapshots `remove_orphan_files` re-reads
# the shared manifests of each one, which is quadratic.
mkdir -p "${TABLE_PATH}/data"
for i in $(seq 10); do
    echo "junk" > "${TABLE_PATH}/data/orphan-${i}.parquet"
    TZ=UTC touch -t 202001010000 "${TABLE_PATH}/data/orphan-${i}.parquet"
done
$SCALE --query_id "${CLICKHOUSE_DATABASE}_orphans" --query "
    ALTER TABLE ${TABLE} EXECUTE remove_orphan_files('$($CH --query "SELECT toString(now())")')" | grep '^deleted_data_files'

$CH --query "SYSTEM FLUSH LOGS query_log"
$CH --query "
    SELECT query_id LIKE '%_expire' ? 'expire_snapshots' : 'remove_orphan_files', ProfileEvents['FileOpen'] BETWEEN 30 AND 120
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id IN ('${CLICKHOUSE_DATABASE}_expire', '${CLICKHOUSE_DATABASE}_orphans') AND type = 'QueryFinish'
    ORDER BY event_time_microseconds"
$CH --query "SELECT count() FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}'"
$CH --query "SELECT count(), sum(id) FROM ${TABLE}"
$CH --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
