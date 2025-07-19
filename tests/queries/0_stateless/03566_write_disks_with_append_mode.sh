#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="${BASH_SOURCE[0]/.sh/.xml}"


function test_append()
{
    disk_name="$1"
    echo "$disk_name"
    printf "%s" "abc" | clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "write 'test.txt'"
    clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "read 'test.txt'"

    printf "%s" "def" | clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "write --mode append 'test.txt'" 2>/dev/null
    clickhouse-disks -C "$config" --disk "$disk_name" --save-logs --query "read 'test.txt'"
}

test_append "test_local_disk_metadata_local_object_storage"
test_append "test_local_disk_metadata_s3_object_storage"
test_append "test_plain_metadata_s3_object_storage"
test_append "test_plain_rewriteable_metadata_s3_object_storage"


