#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="$CUR_DIR/03600_disk_app_plain_rewritable_disks_list_cmd.xml"

function test_copy()
{
    disk_name="$1"

    clickhouse-disks -C "$config" --disk "$disk_name" --query "current_disk_with_path"
    # Disk before adding directories
    clickhouse-disks -C "$config" --disk "$disk_name" --query "ls --recursive"
    # Disk after adding directories
    clickhouse-disks -C "$config" --disk "$disk_name" --query "mkdir hello;"
    clickhouse-disks -C "$config" --disk "$disk_name" --query "cd hello; mkdir world;"
    clickhouse-disks -C "$config" --disk "$disk_name" --query "cd hello/world; mkdir Clickhouse"
    clickhouse-disks -C "$config" --disk "$disk_name" --query "ls --recursive"
    clickhouse-disks -C "$config" --disk "$disk_name" --query "cd hello; current_disk_with_path; cd world; current_disk_with_path; ls; ls --recursive"

    clickhouse-disks -C "$config" --disk "$disk_name" --query "rm -r /hello/"
}

test_copy "test_plain_rewritable_metadata_object_storage"

