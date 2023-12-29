#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="${BASH_SOURCE[0]/.sh/.xml}"

function run_test_for_disk()
{
    local disk=$1 && shift

    echo "$disk"

    clickhouse-disks -C "$config" --disk "$disk" write --input "$config" $CLICKHOUSE_DATABASE/test
    clickhouse-disks -C "$config" --log-level test --disk "$disk" copy $CLICKHOUSE_DATABASE/test $CLICKHOUSE_DATABASE/test.copy |& {
        grep -o -e "Single part upload has completed." -e "Single operation copy has completed."
    }
    clickhouse-disks -C "$config" --disk "$disk" remove $CLICKHOUSE_DATABASE/test
    # NOTE: this is due to "copy" does works like "cp -R from to/" instead of "cp from to"
    clickhouse-disks -C "$config" --disk "$disk" remove $CLICKHOUSE_DATABASE/test.copy/test
    clickhouse-disks -C "$config" --disk "$disk" remove $CLICKHOUSE_DATABASE/test.copy
}

function run_test_copy_from_s3_to_s3(){
    local disk_src=$1 && shift
    local disk_dest=$1 && shift

    echo "copy from $disk_src to $disk_dest"
    clickhouse-disks -C "$config" --disk "$disk_src" write  --input "$config" $CLICKHOUSE_DATABASE/test

    clickhouse-disks -C "$config" --log-level test copy --disk-from "$disk_src" --disk-to "$disk_dest"  $CLICKHOUSE_DATABASE/test $CLICKHOUSE_DATABASE/test.copy |& {
        grep -o -e "Single part upload has completed." -e "Single operation copy has completed."
    }
    clickhouse-disks -C "$config" --disk "$disk_dest" remove $CLICKHOUSE_DATABASE/test.copy/test
    clickhouse-disks -C "$config" --disk "$disk_dest" remove $CLICKHOUSE_DATABASE/test.copy
}

run_test_for_disk s3_plain_native_copy
run_test_for_disk s3_plain_no_native_copy
run_test_copy_from_s3_to_s3 s3_plain_native_copy s3_plain_another
