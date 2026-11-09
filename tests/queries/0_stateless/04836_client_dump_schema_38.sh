#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema reports a named collection that the source query of a dictionary reads, which a load runs, and the one a
# cache disk defined in a table setting reads, which CREATE resolves.

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

config="$work/config.xml"
cat > "$config" <<EOF
<clickhouse>
    <display_secrets_in_show_and_select>1</display_secrets_in_show_and_select>
    <custom_cached_disks_base_directory>$work/caches/</custom_cached_disks_base_directory>
    <storage_configuration>
        <disks>
            <blob><type>local_blob_storage</type><path>$work/blob/</path></blob>
        </disks>
    </storage_configuration>
</clickhouse>
EOF

$CLICKHOUSE_LOCAL --config-file "$config" --path "$work/src" -n -q "
CREATE NAMED COLLECTION ncq AS url = 'file:///dev/null', format = 'TSV', structure = 'k UInt64, v UInt64';
CREATE NAMED COLLECTION ncw AS url = 'file:///dev/null', format = 'TSV', structure = 'k UInt64';
CREATE NAMED COLLECTION ncc AS path = 'cc', max_size = '100Mi';
CREATE DATABASE d;
CREATE TABLE d.src (k UInt64, v UInt64) ENGINE = Memory;
CREATE DICTIONARY d.dq (k UInt64, v UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(QUERY 'SELECT k, v FROM url(ncq)')) LAYOUT(FLAT()) LIFETIME(0);
CREATE DICTIONARY d.dw (k UInt64, v UInt64) PRIMARY KEY k
    SOURCE(CLICKHOUSE(TABLE 'src' DB 'd' WHERE 'k IN (SELECT k FROM url(ncw))')) LAYOUT(FLAT()) LIFETIME(0);
CREATE TABLE d.cached (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS disk = disk(name = 'dc', type = cache, disk = 'blob', cache_name = 'ncc');
CREATE NAMED COLLECTION ncm AS path = 'cm', max_size = '100Mi';
CREATE MATERIALIZED VIEW d.mv_cached ENGINE = MergeTree ORDER BY x
    SETTINGS disk = disk(name = 'dm', type = cache, disk = 'blob', cache_name = 'ncm') AS SELECT k AS x FROM d.src;
"

$CLICKHOUSE_LOCAL --config-file "$config" --path "$work/src" --format_display_secrets_in_show_and_select=1 --dump-schema=d \
    > "$work/dump.sql" 2> "$work/err"
echo "dump rc=$?"
echo "source query warned: $(grep -c 'd.dq depends on named collection ncq,' "$work/err")"
echo "source condition warned: $(grep -c 'd.dw depends on named collection ncw,' "$work/err")"
echo "cache disk warned: $(grep -c 'd.cached depends on named collection ncc,' "$work/err")"
echo "inner cache disk warned: $(grep -c 'd.mv_cached depends on named collection ncm,' "$work/err")"
echo "collections emitted: $(grep -c 'CREATE NAMED COLLECTION' "$work/dump.sql")"

