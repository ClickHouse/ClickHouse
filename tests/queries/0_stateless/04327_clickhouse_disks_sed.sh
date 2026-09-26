#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="$CUR_DIR/04327_clickhouse_disks_sed.xml"

# Established isolated per-test directories.
local_disk_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_sed_local"
object_storage_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_sed_object_storage"

mkdir -p "$local_disk_dir" "$object_storage_dir/data" "$object_storage_dir/metadata"

export TEST_DISK_04327_SED_LOCAL_PATH="$(realpath "$local_disk_dir")/"
export TEST_DISK_04327_SED_OBJECT_STORAGE_PATH="$(realpath "$object_storage_dir/data")/"
export TEST_DISK_04327_SED_OBJECT_STORAGE_METADATA_PATH="$(realpath "$object_storage_dir/metadata")/"

run() {
    clickhouse-disks -C "$config" --disk "$disk_name" --query "$1"
}

test_sed() {
    disk_name="$1"
    file="${CLICKHOUSE_TEST_UNIQUE_NAME}_${disk_name}.txt"

    echo "=== $disk_name ==="

    # Substitution applied in place.
    printf 'foo\nfoo bar\nbaz\n' | run "write $file"
    run "sed 's/foo/QUX/g' $file"
    echo "--- after substitution ---"
    run "read $file"

    # Deletion across lines (confirms streaming over multiple lines).
    run "sed '/baz/d' $file"
    echo "--- after deletion ---"
    run "read $file"

    # A bad expression must fail and leave the original file untouched (fail-close).
    echo "--- bad expression error ---"
    run "sed 's/[/x/' $file" 2>&1 | grep -o "sed wrote to stderr" | head -n 1
    echo "--- unchanged after failed sed ---"
    run "read $file"

    run "remove $file"
}

test_sed "test_disk_04327_sed_local"
test_sed "test_disk_04327_sed_object_storage"
