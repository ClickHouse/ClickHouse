#!/usr/bin/env bash

# Files that a part keeps inside packed archives (statistics, skip indices, packed part storage)
# must be readable through the userspace page cache: https://github.com/ClickHouse/ClickHouse/issues/122157

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

work_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${work_dir}"
mkdir -p "${work_dir}"

# The CI server has no page cache configured, so each case runs a private clickhouse-local with one.
> "${work_dir}/config.yaml" echo "
page_cache_max_size: 134217728
custom_local_disks_base_directory: ${work_dir}/
"

function run()
{
    local name=$1 table_settings=$2
    shift 2
    echo "-- ${name}"
    ${CLICKHOUSE_LOCAL} --path "${work_dir}/${name}" --config-file "${work_dir}/config.yaml" "$@" --multiquery "
        CREATE TABLE t (key UInt64, n UInt64, INDEX i_mm n TYPE minmax GRANULARITY 1)
        ENGINE = MergeTree ORDER BY key SETTINGS packed_skip_index_max_bytes = 1048576, ${table_settings};
        INSERT INTO t SELECT number, number * 3 FROM numbers(20000);
        SELECT part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;
        SELECT count(), sum(n) FROM t WHERE n BETWEEN 3000 AND 9000;
        SELECT value > 0 FROM system.events WHERE event = 'PageCacheReadBytes';"
}

local_disk=(--use_page_cache_for_local_disks 1 --local_filesystem_read_method pread)

run full "min_bytes_for_full_part_storage = 0" "${local_disk[@]}"
run packed "min_bytes_for_full_part_storage = 1073741824" "${local_disk[@]}"
run object_storage "min_bytes_for_full_part_storage = 0, disk = disk(type = object_storage,
    object_storage_type = local_blob_storage, metadata_type = local,
    metadata_path = '${work_dir}/blob_metadata/', path = '${work_dir}/blob_data/')" \
    --use_page_cache_for_disks_without_file_cache 1 --remote_filesystem_read_method read

rm -rf "${work_dir}"
