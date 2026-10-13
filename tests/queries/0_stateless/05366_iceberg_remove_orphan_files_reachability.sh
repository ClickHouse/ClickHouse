#!/usr/bin/env bash
# `remove_orphan_files` must delete only files no snapshot can reach, even with `older_than` set to now:
# data and delete files of every snapshot, the metadata log and the version hint survive, and every
# snapshot stays readable through time travel.
#
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading format version 3 metadata logs a spurious "violates Iceberg specification" warning,
# https://github.com/ClickHouse/ClickHouse/issues/125333
CH="${CLICKHOUSE_CLIENT} --send_logs_level=error --session_timezone=UTC --allow_insert_into_iceberg=1 --allow_iceberg_remove_orphan_files=1 --iceberg_orphan_files_min_retention_seconds=0"

# Prints only non-zero metrics, so the reference shows what was (or would be) deleted.
remove_orphans()
{
    $CH --query "ALTER TABLE $1 EXECUTE remove_orphan_files($2)" | grep -v $'\t0$'
}

snapshot_contents()
{
    for snapshot in $($CH --query "SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = '$1' ORDER BY made_current_at"); do
        $CH --query "SELECT count(), sum(id) FROM $1 SETTINGS iceberg_snapshot_id = $snapshot"
    done
}

# The writer never lists v1.metadata.json in metadata-log, so cleanup may remove it.
list_files() { (cd "${TABLE_PATH}" && find . -type f | grep -v '/v1.metadata.json' | sort); }

for format_version in 2 3; do
    echo "format version ${format_version}"
    TABLE="t_${CLICKHOUSE_DATABASE}_v${format_version}"
    TABLE_PATH="${USER_FILES_PATH}/${TABLE}"

    $CH --query "
        CREATE TABLE ${TABLE} (id Int64, p Int64)
        ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet')
        PARTITION BY p
        SETTINGS iceberg_format_version = ${format_version}, allow_experimental_iceberg_compaction = 0"
    $CH --query "INSERT INTO ${TABLE} VALUES (1, 1), (2, 2)"
    $CH --query "INSERT INTO ${TABLE} VALUES (3, 1)"
    # ClickHouse writes position deletes only for format version 2.
    [ "${format_version}" = 2 ] && $CH --mutations_sync=2 --query "ALTER TABLE ${TABLE} DELETE WHERE id = 2"
    $CH --query "INSERT INTO ${TABLE} VALUES (4, 3)"
    # ClickHouse does not write a version hint here; plant one with the right value to check it is kept.
    printf '%s' "$(find "${TABLE_PATH}/metadata" -name '*.metadata.json' | wc -l | tr -d ' ')" > "${TABLE_PATH}/metadata/version-hint.text"

    before=$(snapshot_contents "${TABLE}")
    reachable=$(list_files)

    # Orphans: two from 2020, one exactly at the cutoff, one from 2100 (a writer with a skewed clock).
    for name in data/orphan-old.parquet metadata/orphan-old.avro metadata/v0.metadata.json data/orphan-at-cutoff.parquet data/orphan-future.parquet; do
        echo "junk" > "${TABLE_PATH}/${name}"
    done
    TZ=UTC touch -t 202001010000 "${TABLE_PATH}/data/orphan-old.parquet" "${TABLE_PATH}/metadata/orphan-old.avro" "${TABLE_PATH}/metadata/v0.metadata.json"
    TZ=UTC touch -t 202301010000 "${TABLE_PATH}/data/orphan-at-cutoff.parquet"
    TZ=UTC touch -t 210001010000 "${TABLE_PATH}/data/orphan-future.parquet"

    echo "dry run, cutoff in UTC+14 is before the cutoff file"
    $CH --session_timezone=Pacific/Kiritimati --query "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files(older_than = '2023-01-01 00:00:00', dry_run = 1)" | grep -v $'\t0$'
    echo "dry run, cutoff in UTC-10 is after the cutoff file"
    $CH --session_timezone=Pacific/Honolulu --query "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files(older_than = '2023-01-01 00:00:00', dry_run = 1)" | grep -v $'\t0$'

    echo "cutoff equal to the file mtime keeps the file"
    remove_orphans "${TABLE}" "older_than = '2023-01-01 00:00:00'"
    # Metrics are not printed here: they include v1.metadata.json, which the writer leaves out of metadata-log.
    now=$($CH --query "SELECT toString(now())")
    remove_orphans "${TABLE}" "older_than = '${now}'" > /dev/null
    echo "cutoff now, then a second run deletes nothing"
    remove_orphans "${TABLE}" "older_than = '${now}'"

    echo "reachable files deleted:"
    comm -23 <(echo "${reachable}") <(list_files)
    echo "orphans left:"
    comm -13 <(echo "${reachable}") <(list_files)
    [ "${before}" = "$(snapshot_contents "${TABLE}")" ] && echo "all snapshots unchanged"
    $CH --query "SELECT arraySort(groupArray(id)) FROM ${TABLE}"

    $CH --query "DROP TABLE ${TABLE}"
    rm -rf "${TABLE_PATH}"
done

echo "scale"
TABLE="t_${CLICKHOUSE_DATABASE}_scale"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}"
$CH --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet') PARTITION BY icebergBucket(50, id) SETTINGS allow_experimental_iceberg_compaction = 0"
for i in 1 2; do
    $CH --query "INSERT INTO ${TABLE} SELECT number FROM numbers(1000)"
done
for i in $(seq 200); do
    echo "junk" > "${TABLE_PATH}/data/orphan-${i}.parquet"
done
TZ=UTC touch -t 202001010000 "${TABLE_PATH}"/data/orphan-*.parquet
remove_orphans "${TABLE}" "older_than = '2023-01-01 00:00:00'"
find "${TABLE_PATH}/data" -name '*.parquet' | wc -l | tr -d ' '
$CH --query "SELECT count(), sum(id) FROM ${TABLE}"
$CH --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"

echo "access control"
TABLE="t_${CLICKHOUSE_DATABASE}_acl"
USER="u_${CLICKHOUSE_DATABASE}"
$CH --query "
    CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${USER_FILES_PATH}/${TABLE}/', 'Parquet');
    INSERT INTO ${TABLE} VALUES (1);
    DROP USER IF EXISTS ${USER};
    CREATE USER ${USER};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.${TABLE} TO ${USER};"
${CLICKHOUSE_CLIENT} --user "${USER}" --allow_iceberg_remove_orphan_files=1 --allow_insert_into_iceberg=1 \
    --query "ALTER TABLE ${CLICKHOUSE_DATABASE}.${TABLE} EXECUTE remove_orphan_files(dry_run = 1)" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CH --query "GRANT ALTER TABLE EXECUTE ON ${CLICKHOUSE_DATABASE}.${TABLE} TO ${USER}"
${CLICKHOUSE_CLIENT} --user "${USER}" --allow_iceberg_remove_orphan_files=1 --allow_insert_into_iceberg=1 \
    --query "ALTER TABLE ${CLICKHOUSE_DATABASE}.${TABLE} EXECUTE remove_orphan_files(dry_run = 1)" > /dev/null && echo "granted"
$CH --allow_iceberg_remove_orphan_files=0 --query "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files(dry_run = 1)" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'
$CH --query "DROP USER ${USER}; DROP TABLE ${TABLE}"
rm -rf "${USER_FILES_PATH:?}/${TABLE}"
