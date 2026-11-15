#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="$CUR_DIR/03600_disk_app_plain_rewritable_disks_list_cmd.xml"
prefix="$CLICKHOUSE_TEST_UNIQUE_NAME"

function test_copy()
{
    disk_name="$1"

    clickhouse-disks -C "$config" --disk "$disk_name" --query "mkdir ${prefix}_hello; cd ${prefix}_hello; mkdir ${prefix}_world; cd ${prefix}_world; mkdir ${prefix}_Clickhouse; mkdir ${prefix}_Cloud"
    printf "Test file can't be navigated" | clickhouse-disks -C "$config" --disk "$disk_name" --query "cd ${prefix}_hello; write ${prefix}_test_file.txt;"
    clickhouse-disks -C "$config" --disk "$disk_name" --query "cd ${prefix}_hello; read ${prefix}_test_file.txt;"
    clickhouse-disks -C "$config" --disk "$disk_name" --query "cd ${prefix}_hello; cd ${prefix}_test_file.txt;"  2>&1 | grep -o "BAD_ARGUMENTS"
    echo "Test can't navigate partial path"
    clickhouse-disks -C "$config" --disk "$disk_name" --query "cd ${prefix};"  2>&1 | grep -o "BAD_ARGUMENTS"
    echo "Test ls with implicit path"
    clickhouse-disks -C "$config" --disk "$disk_name" --query "cd ${prefix}_hello; ls; ls --recursive;"
    echo "Test ls with explicit path"
    clickhouse-disks -C "$config" --disk "$disk_name" --query "ls ${prefix}_hello; ls --recursive ${prefix}_hello"
    clickhouse-disks -C "$config" --disk "$disk_name" --query "rm -r /${prefix}_hello/"
}

test_copy "test_plain_rewritable_metadata_object_storage"

