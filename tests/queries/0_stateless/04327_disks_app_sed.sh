#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="$CUR_DIR/04327_disks_app_sed.xml"

# Root the local test disk in an isolated, writable per-test directory instead of `/`.
# $CLICKHOUSE_TMP can be relative, so resolve to the absolute path the local disk requires.
local_disk_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_sed_local"
mkdir -p "$local_disk_dir"
export TEST_DISK_04327_SED_LOCAL_PATH="$(realpath "$local_disk_dir")/"

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
    run "sed 's/[/x/' $file" 2>&1 | grep -o "unterminated \`s' command" | head -n 1
    echo "--- unchanged after failed sed ---"
    run "read $file"

    run "remove $file"
}

test_sed "test_disk_04327_sed_local"
test_sed "test_disk_04327_sed_object_storage"
