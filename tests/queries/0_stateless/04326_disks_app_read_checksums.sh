#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings
# The test asserts on the exact set of files inside a part, which depends on the
# part's physical layout. Disable MergeTree settings randomization so the part
# always uses the build defaults (a compact part: data.bin, data.cmrk4, ...).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="$CUR_DIR/04326_disks_app_read_checksums.xml"
disk_name="test_disk_04326_read_checksums"

run() {
    clickhouse-disks -C "$config" --disk "$disk_name" --query "$1" 2>/dev/null
}

# Create a MergeTree part.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS rc_test"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE rc_test (k UInt64, s String) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} --query "INSERT INTO rc_test SELECT number, toString(number) FROM numbers(3)"
path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts \
    WHERE database = currentDatabase() AND table = 'rc_test' AND active LIMIT 1")

# Verify file names.
echo "--- file names ---"
run "read-checksums ${path}checksums.txt" | tail -n +2 | awk '{print $1}' | sort

# count.txt is uncompressed and holds fixed content, so its file_size and file_hash are stable.
# (The compressed files' sizes/hashes depend on the build's compression and are not asserted.)
echo "--- count.txt: name file_size file_hash ---"
run "read-checksums ${path}checksums.txt" | awk '$1=="count.txt" {print $1, $2, $3}'

${CLICKHOUSE_CLIENT} --query "DROP TABLE rc_test"
