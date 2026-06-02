#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="$CUR_DIR/04304_disks_app_wc.xml"
disk_name="test_disk_04304_wc"

run() {
    clickhouse-disks -C "$config" --disk "$disk_name" --query "$1" 2>/dev/null
}

# A file with two newline-terminated lines: 2 lines, 5 words, 24 bytes.
file="wc_multiline.txt"
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
file="wc_no_newline.txt"
printf "abc def" | run "write $file"
echo "--- no trailing newline ---"
run "wc $file"
run "remove $file"

# An empty file: 0 lines, 0 words, 0 bytes.
file="wc_empty.txt"
printf "" | run "write $file"
echo "--- empty ---"
run "wc $file"
run "remove $file"
