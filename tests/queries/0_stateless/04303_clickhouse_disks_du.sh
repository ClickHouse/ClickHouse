#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="$CUR_DIR/04303_clickhouse_disks_du.xml"
disk="test_disk_04303"
dir="$CLICKHOUSE_TEST_UNIQUE_NAME"

function disks()
{
    clickhouse-disks -C "$config" --disk "$disk" --save-logs --query "$1" 2>/dev/null
}

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

# Clean up.
disks "remove -r $dir"
