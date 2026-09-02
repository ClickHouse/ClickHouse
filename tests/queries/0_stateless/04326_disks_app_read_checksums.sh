#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings
# The test asserts on the exact set of files inside a part, which depends on the
# part's physical layout. Disable MergeTree settings randomization so the part
# always uses the build defaults (a compact part: data.bin, data.cmrk4, ...).
#
# The table is pinned to the local `default` disk (see CREATE TABLE below). Under
# object-storage default policies (e.g. when the CI job runs with `--s3-storage`)
# a part's local `checksums.txt` is object-storage metadata, not a real part
# `checksums.txt`, so `read-checksums` would have nothing to parse. The `default`
# disk is always a plain local disk and is not overridden by those policies.

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
${CLICKHOUSE_CLIENT} --query "CREATE TABLE rc_test (k UInt64, s String) ENGINE = MergeTree ORDER BY k SETTINGS disk = 'default'"
# Pin materialize_statistics_on_insert (randomized in CI): when on, auto-statistics are
# materialized into the part as statistics.packed, which would change the asserted file set.
${CLICKHOUSE_CLIENT} --query "INSERT INTO rc_test SETTINGS materialize_statistics_on_insert = 0 SELECT number, toString(number) FROM numbers(3)"
path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts \
    WHERE database = currentDatabase() AND table = 'rc_test' AND active LIMIT 1")

# Verify file names.
echo "--- file names ---"
run "read-checksums ${path}checksums.txt" | tail -n +2 | awk '{print $1}' | sort

# count.txt is uncompressed and holds fixed content, so its file_size and file_hash are stable.
# (The compressed files' sizes/hashes depend on the build's compression and are not asserted.)
echo "--- count.txt: name file_size file_hash ---"
run "read-checksums ${path}checksums.txt" | awk '$1=="count.txt" {print $1, $2, $3}'

# Negative test: format version 1 is too old and unsupported. The command must reject it
# instead of printing an empty table.
echo "--- unsupported format version 1 ---"
bad_checksums="$(mktemp -p "${CLICKHOUSE_TMP}" rc_v1.XXXXXX)"
printf 'checksums format version: 1\n' > "$bad_checksums"
# The disk root is `/`, so the absolute path is reachable from the disk.
clickhouse-disks -C "$config" --disk "$disk_name" --query "read-checksums ${bad_checksums}" 2>&1 >/dev/null \
    | grep -o "is not a valid checksums file or uses an unsupported (too old) format" | head -n 1
rm -f "$bad_checksums"

${CLICKHOUSE_CLIENT} --query "DROP TABLE rc_test"
