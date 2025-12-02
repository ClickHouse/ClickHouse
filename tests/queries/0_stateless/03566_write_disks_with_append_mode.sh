#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="$CUR_DIR/03566_write_disks_with_append_mode.xml"

function test_append()
{
    disk_name="$1"
    echo "$disk_name"
    file_name="file_03566_$(random_str 10).txt"
    printf "%s" "abc" | clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "write $file_name"
    clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "read $file_name"

    printf "%s" "123" | clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "write --mode append $file_name" 2>/dev/null
    clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "read $file_name"

    printf "%s" "def" | clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "write --mode append $file_name" 2>/dev/null
    clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "read $file_name"

    clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "remove $file_name"
}

test_append "test_local_disk_metadata_local_object_storage"
test_append "test_local_disk_metadata_s3_object_storage"
test_append "test_plain_metadata_s3_object_storage"
test_append "test_plain_rewriteable_metadata_s3_object_storage"


