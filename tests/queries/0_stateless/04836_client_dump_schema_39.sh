#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema loads a `plain_rewritable` disk without writing to it: no metadata snapshot is written.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT
cd "$work" || exit 1

cat > "$work/config.xml" <<XML
<clickhouse>
    <storage_configuration>
        <disks>
            <pr>
                <type>object_storage</type>
                <object_storage_type>local</object_storage_type>
                <metadata_type>plain_rewritable</metadata_type>
                <path>$work/disk/</path>
            </pr>
        </disks>
        <policies><pr><volumes><main><disk>pr</disk></main></volumes></pr></policies>
    </storage_configuration>
</clickhouse>
XML

$CLICKHOUSE_LOCAL --config-file "$work/config.xml" --path "$work/src" -n -q "CREATE DATABASE d;
    CREATE TABLE d.t (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'pr';
    INSERT INTO d.t SELECT number FROM numbers(10);"
echo "snapshot after the insert: $(ls "$work/disk/__meta" | grep -c '^snapshot.bin$')"

# Without the snapshot, a writer of the disk would write it again when it loads.
rm "$work/disk/__meta/snapshot.bin"
(cd "$work/disk" && find . -type f | sort) > "$work/before.txt"
$CLICKHOUSE_LOCAL --config-file "$work/config.xml" --path "$work/src" --dump-schema d > "$work/dump.sql" 2> "$work/err.txt" \
    || echo "dump failed: $(cat "$work/err.txt")"
echo "table dumped: $(grep -c 'CREATE TABLE d.t' "$work/dump.sql")"
echo "snapshot after the dump: $(ls "$work/disk/__meta" | grep -c '^snapshot.bin$')"
(cd "$work/disk" && find . -type f | sort) > "$work/after.txt"
cmp -s "$work/before.txt" "$work/after.txt" && echo "disk files unchanged" || echo "disk files changed"
