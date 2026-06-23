#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="$CUR_DIR/04304_clickhouse_disks_wc.xml"
disk_name="test_disk_04304_wc"

disk_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_wc"
mkdir -p "$disk_dir/data" "$disk_dir/metadata"
export TEST_DISK_04304_WC_PATH="$(realpath "$disk_dir/data")/"
export TEST_DISK_04304_WC_METADATA_PATH="$(realpath "$disk_dir/metadata")/"

dir="$CLICKHOUSE_TEST_UNIQUE_NAME"

run() {
    clickhouse-disks -C "$config" --disk "$disk_name" --query "$1" 2>/dev/null
}

# Start from a clean working directory (in case of a previous re-run).
run "remove -r $dir"
# The `write` command does not create parent directories, so create them first.
run "mkdir --parents $dir"

# A file with two newline-terminated lines: 2 lines, 5 words, 24 bytes.
file="$dir/wc_multiline.txt"
printf "hello world\nfoo bar baz\n" | run "write $file"
echo "--- full ---"
run "wc $file"
echo "--- bytes ---"
run "wc -c $file"
echo "--- lines ---"
run "wc -l $file"
echo "--- words ---"
run "wc -w $file"
echo "--- lines+words ---"
run "wc -l -w $file"
run "remove $file"

# A file without a trailing newline: 0 lines, 2 words, 7 bytes.
file="$dir/wc_no_newline.txt"
printf "abc def" | run "write $file"
echo "--- no trailing newline ---"
run "wc $file"
run "remove $file"

# An empty file: 0 lines, 0 words, 0 bytes.
file="$dir/wc_empty.txt"
printf "" | run "write $file"
echo "--- empty ---"
run "wc $file"
run "remove $file"

# A directory is not a regular file: wc should fail rather than count.
subdir="$dir/wc_dir"
run "mkdir $subdir"
echo "--- directory ---"
clickhouse-disks -C "$config" --disk "$disk_name" --query "wc $subdir" 2>&1 >/dev/null \
    | grep -o "is a directory" | head -n1

# Clean up.
run "remove -r $dir"
