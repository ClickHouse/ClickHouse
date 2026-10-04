#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_V1="v1_${CLICKHOUSE_DATABASE}"
TABLE_V2="v2_${CLICKHOUSE_DATABASE}"
TABLE_PARTITIONED="part_${CLICKHOUSE_DATABASE}"
PATH_V1="${USER_FILES_PATH}/${TABLE_V1}/"
PATH_V2="${USER_FILES_PATH}/${TABLE_V2}/"
PATH_PARTITIONED="${USER_FILES_PATH}/${TABLE_PARTITIONED}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS ${TABLE_V1};
        DROP TABLE IF EXISTS ${TABLE_V2};
        DROP TABLE IF EXISTS ${TABLE_PARTITIONED}"
    rm -rf "${PATH_V1}" "${PATH_V2}" "${PATH_PARTITIONED}"
}
trap cleanup EXIT

ROLLOVER="iceberg_insert_max_rows_in_data_file = 2, max_insert_threads = 1, max_block_size = 2,
          max_insert_block_size = 2, min_insert_block_size_rows = 2, min_insert_block_size_bytes = 0"

# Every manifest-list entry must report how many manifest entries the manifest it points at really
# has. One INSERT rolls over into several data files that all land in the same manifest, so the
# count is not always 1, and a reader that trusts it plans the scan with the wrong number of files.
check()
{
    local label=$1
    local table_path=$2

    local snapshot
    snapshot=$(find "${table_path}metadata" -maxdepth 1 -name 'snap-*.avro' -type f -printf '%T@ %p\n' \
        | sort -n | tail -1 | cut -d' ' -f2-)

    ${CLICKHOUSE_CLIENT} --query "
        SELECT splitByChar('/', manifest_path)[-1], added_files_count, existing_files_count
        FROM file('${snapshot}', Avro)
    " | while IFS=$'\t' read -r manifest added existing; do
        entries=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM file('${table_path}metadata/${manifest}', Avro)")
        echo -e "${label}\tadded_files_count=${added}\tmanifest_entries=${entries}\tmatches=$([ "${added}" = "${entries}" ] && echo yes || echo "no (existing_files_count=${existing})")"
    done | sort
}

for version in 1 2; do
    table="v${version}_${CLICKHOUSE_DATABASE}"
    table_path="${USER_FILES_PATH}/${table}/"

    # Reading back a v1 table logs a warning about the writing engine, which is a different bug.
    client="${CLICKHOUSE_CLIENT} --send_logs_level=fatal"

    ${client} --query "
        CREATE TABLE ${table} (n UInt64) ENGINE = IcebergLocal('${table_path}')
        SETTINGS iceberg_format_version = ${version}"

    ${client} --allow_insert_into_iceberg=1 --query "
        INSERT INTO ${table} SELECT number FROM numbers(10) SETTINGS ${ROLLOVER}"

    ${client} --query "
        SELECT 'v${version}', 'data_files', countIf(content = 'DATA'), 'snapshots', uniqExact(snapshot_id)
        FROM system.iceberg_files WHERE database = currentDatabase() AND table = '${table}'"

    check "v${version}" "${table_path}"
done

# One manifest per partition key, each with its own number of data files.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_PARTITIONED} (n UInt64, p UInt64)
    ENGINE = IcebergLocal('${PATH_PARTITIONED}') PARTITION BY (p)"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE_PARTITIONED} SELECT number, number % 2 FROM numbers(10) SETTINGS ${ROLLOVER}"

check "partitioned" "${PATH_PARTITIONED}"

# The mutation path writes one data file per manifest and must keep reporting 1.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query "
    DELETE FROM ${TABLE_V2} WHERE n = 3"

check "after delete" "${PATH_V2}"

${CLICKHOUSE_CLIENT} --query "SELECT 'rows', count() FROM ${TABLE_V2}"
