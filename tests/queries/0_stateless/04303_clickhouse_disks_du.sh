#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="$CUR_DIR/04303_clickhouse_disks_du.xml"

base_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p \
    "$base_dir/local/data" "$base_dir/local/metadata" \
    "$base_dir/plain" \
    "$base_dir/plain_rewritable"
export TEST_DISK_04303_PATH="$(realpath "$base_dir/local/data")/"
export TEST_DISK_04303_METADATA_PATH="$(realpath "$base_dir/local/metadata")/"
export TEST_DISK_PLAIN_04303_PATH="$(realpath "$base_dir/plain")/"
export TEST_DISK_PLAIN_REWRITABLE_04303_PATH="$(realpath "$base_dir/plain_rewritable")/"

dir="$CLICKHOUSE_TEST_UNIQUE_NAME"

function disks()
{
    clickhouse-disks -C "$config" --disk "$disk" --save-logs --query "$1" 2>/dev/null
}

# Run the same `du` scenario against a given disk. The behaviour should be identical across
# the `local`, `plain`, and `plain_rewritable` metadata backends.
function run_du_test()
{
    local disk="$1"
    echo "# $disk"

    # Start from a clean working directory (in case of a previous re-run).
    disks "remove -r $dir"

    # The `write` command does not create parent directories, so create them first.
    disks "mkdir --parents $dir/sub"

    # A single 5-byte file.
    printf "%s" "abcde" | disks "write $dir/file5"

    # A subdirectory holding a 5-byte and a 10-byte file (15 bytes total).
    printf "%s" "abcde" | disks "write $dir/sub/a"
    printf "%s" "abcdefghij" | disks "write $dir/sub/b"

    # Size of a single file: raw bytes and human-readable.
    disks "du $dir/file5"
    disks "du -h $dir/file5"

    # Size of a directory: sum of all files recursively.
    disks "du $dir/sub"
    disks "du -h $dir/sub"

    # Size of the whole directory tree: file5 (5) + sub (15) = 20.
    disks "du $dir"

    # Human-readable formatting with a larger unit: 1536 bytes -> 1.50 KiB.
    printf 'x%.0s' $(seq 1536) | disks "write $dir/big"
    disks "du -h $dir/big"

    # A nonexistent path reports a clear error rather than a low-level failure.
    clickhouse-disks -C "$config" --disk "$disk" --query "du $dir/does_not_exist" 2>&1 >/dev/null \
        | grep -o "doesn't exist" | head -n1

    # Must get a similar error for a nonexistent path that is a prefix of a valid path.
    clickhouse-disks -C "$config" --disk "$disk" --query "du $dir/file" 2>&1 >/dev/null \
        | grep -o "doesn't exist" | head -n1

    # Navigate to the subdirectory and run `du` on a file by its relative name.
    disks "cd $dir/sub; du -h a"

    # Clean up.
    disks "remove -r $dir"
}

run_du_test "test_disk_04303"
run_du_test "test_disk_plain_04303"
run_du_test "test_disk_plain_rewritable_04303"
run_du_test "test_disk_s3_plain_04303"
