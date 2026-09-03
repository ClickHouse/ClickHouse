#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="$CUR_DIR/03578_copy_file_in_object_storage.xml"

function test_copy()
{
    disk_name="$1"
    echo "$disk_name"
    file_name_src="file_src_03578_$(random_str 10).txt"
    file_name_existing_target="file_existing_target_03578_$(random_str 10).txt"
    file_name_non_existing_target="file_non_existing_target_03578_$(random_str 10).txt"

    printf "%s" "abc" | clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "write $file_name_src"
    printf "%s" "123" | clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "write --mode append $file_name_src" 2>/dev/null
    printf "%s" "def" | clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "write --mode append $file_name_src" 2>/dev/null
    clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "read $file_name_src"

    printf "%s" "xyz" | clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "write $file_name_existing_target"


    clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "cp --path-from $file_name_src --path-to $file_name_existing_target"
    clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "read $file_name_existing_target"

    clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "cp --path-from $file_name_src --path-to $file_name_non_existing_target"
    clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "read $file_name_non_existing_target"

    clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "remove $file_name_src"
    clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "remove $file_name_existing_target"
    clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "remove $file_name_non_existing_target"
}

test_copy "test_local_disk_metadata_local_object_storage"
test_copy "test_local_disk_metadata_s3_object_storage"
# For these following disks, adding new blob to metadata is not supported, when appending, it fails.
test_copy "test_plain_metadata_s3_object_storage"
test_copy "test_plain_rewriteable_metadata_s3_object_storage"


