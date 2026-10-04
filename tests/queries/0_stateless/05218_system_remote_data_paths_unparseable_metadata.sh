#!/usr/bin/env bash
# Tests how `system.remote_data_paths` treats a metadata file it cannot read: an empty one is
# skipped, one that is not object metadata at all fails the read and names the file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

work_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_rdp"
rm -rf "${work_dir}"
mkdir -p "${work_dir}"

# A private instance: a metadata file planted below is visible to every reader of this disk.
# The disk is a custom one, so its paths must lie inside `custom_local_disks_base_directory`.
local_opts=(--path "${work_dir}/db")
local_config=(-- --custom_local_disks_base_directory="${work_dir}/")

${CLICKHOUSE_LOCAL} "${local_opts[@]}" --query "
CREATE TABLE t (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    name = 05218_disk,
    type = object_storage,
    object_storage_type = local_blob_storage,
    metadata_type = local,
    metadata_path = '${work_dir}/meta/',
    path = '${work_dir}/blobs/');
INSERT INTO t SELECT number FROM numbers(10);
" "${local_config[@]}"

echo "-- the table's blobs are reported"
${CLICKHOUSE_LOCAL} "${local_opts[@]}" --query "SELECT count() > 0 FROM system.remote_data_paths WHERE disk_name = '05218_disk'" "${local_config[@]}"

echo "-- an empty metadata file, as an interrupted write leaves behind, is skipped"
: > "${work_dir}/meta/store/empty_metadata"
${CLICKHOUSE_LOCAL} "${local_opts[@]}" --query "SELECT count() > 0 FROM system.remote_data_paths WHERE disk_name = '05218_disk'" "${local_config[@]}"

echo "-- a file that is not object metadata fails the read, naming the file"
echo 'not object metadata' > "${work_dir}/meta/store/broken_metadata"
${CLICKHOUSE_LOCAL} "${local_opts[@]}" --query "SELECT count() FROM system.remote_data_paths WHERE disk_name = '05218_disk'" "${local_config[@]}" 2>&1 \
    | grep -c 'While parsing file store/broken_metadata'

rm -rf "${work_dir}"
