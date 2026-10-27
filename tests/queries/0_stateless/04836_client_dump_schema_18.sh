#!/usr/bin/env bash

# --dump-schema emits `allow_archive_path_syntax` for an archive path (`a.tar :: data.csv`) in a table function that
# CREATE parses: the one a table is created AS, and one in the SELECT of a materialized view. A plain view parses its
# SELECT only when it is read, under the reader's setting, so it does not need the gate. A `URL` or `S3` call can also
# take the path from a named collection. Every dump is replayed in a session that has the setting at 0.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Creates the objects of `$3` in a fresh source and dumps them, then replays the dump followed by the queries of `$4`
# in one session of a fresh target with `allow_archive_path_syntax = 0`, printing the first error code of the replay,
# and whether the replayed `d.$2` has the definition of the source one. The queries run in the replaying session
# because a table keeps the source it parsed only until it is attached again.
round_trip() {
    local name=$1 object=$2 ddl=$3 queries=$4
    rm -rf "$work/src" "$work/dst"
    $CLICKHOUSE_LOCAL --path "$work/src" -n -q "CREATE DATABASE d; $ddl"
    $CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > "$work/$name.sql" 2>/dev/null
    echo "$name: dump rc=$?, archive gate: $(grep -c '^SET allow_archive_path_syntax = 1;' "$work/$name.sql")"
    { cat "$work/$name.sql"; echo "$queries"; } > "$work/$name.replay.sql"
    $CLICKHOUSE_LOCAL --path "$work/dst" --allow_archive_path_syntax=0 -n --queries-file "$work/$name.replay.sql" 2>&1 \
        | grep -o 'Code: [0-9]*' | head -1
    echo "$name: replay rc=${PIPESTATUS[0]}"
    local source_definition replayed_definition
    source_definition=$($CLICKHOUSE_LOCAL --path "$work/src" -q "SHOW CREATE TABLE d.$object")
    replayed_definition=$($CLICKHOUSE_LOCAL --path "$work/dst" -q "SHOW CREATE TABLE d.$object")
    echo "$name: same definition: $([ -n "$source_definition" ] && [ "$source_definition" = "$replayed_definition" ] && echo 1 || echo 0)"
}

# The table keeps the source its CREATE parsed. Writing to an archive is refused (code 48, `NOT_IMPLEMENTED`), while a
# table that took the whole string for a plain file name would write a file called `a.tar :: data.csv`.
round_trip as_file t "CREATE TABLE d.t (x UInt64) AS file('$work/a.tar :: data.csv', CSV, 'x UInt64');" \
    "INSERT INTO d.t VALUES (1);"
echo "as_file: source table refuses the write: $($CLICKHOUSE_LOCAL --path "$work/src" -q "INSERT INTO d.t VALUES (1)" 2>&1 | grep -o 'Code: [0-9]*')"
echo "as_file: plain files written: $(find "$work" -maxdepth 1 -name 'a.tar*' | wc -l)"

# A path without an archive does not need the gate.
round_trip as_plain_file t "CREATE TABLE d.t (x UInt64) AS file('$work/data.csv', CSV, 'x UInt64');" ""

# Creating a materialized view analyzes its SELECT, which parses the table function under the replaying session.
round_trip materialized_view mv "
    CREATE TABLE d.src (x UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW d.mv ENGINE = Memory
        AS SELECT x FROM d.src WHERE x IN (SELECT x FROM file('$work/a.tar :: data.csv', CSV, 'x UInt64'));" ""

# A plain view stores its columns, so its replay does not parse the table function at all.
round_trip view v "CREATE VIEW d.v AS SELECT x FROM file('$work/a.tar :: data.csv', CSV, 'x UInt64');" ""

# A `URL` or `S3` table reads its location from the `url` of a named collection it names, so the dump looks for an archive
# path there too. A session that sees the values masked cannot tell, so it emits the gate anyway.
nc_users="$work/nc_users.xml"
nc_config="$work/nc.xml"
cat > "$nc_users" <<XML
<clickhouse>
    <profiles>
        <default><format_display_secrets_in_show_and_select>1</format_display_secrets_in_show_and_select></default>
    </profiles>
    <users>
        <default>
            <password></password>
            <profile>default</profile>
            <quota>default</quota>
            <named_collection_control>1</named_collection_control>
            <show_named_collections_secrets>1</show_named_collections_secrets>
        </default>
    </users>
    <quotas><default></default></quotas>
</clickhouse>
XML
cat > "$nc_config" <<XML
<clickhouse>
    <users_config>${nc_users}</users_config>
    <display_secrets_in_show_and_select>1</display_secrets_in_show_and_select>
</clickhouse>
XML

# Dumps a `URL` table over a named collection with the URL `$2`, from a session that sees the collection's values if `$3`
# is `shown`, and replays the dump with `allow_archive_path_syntax = 0` where the collection already exists.
named_collection_round_trip() {
    local name=$1 url=$2 visibility=$3
    local collection="CREATE NAMED COLLECTION nc AS url = '$url';"
    local dump_options=()
    [ "$visibility" = shown ] && dump_options=(--config-file "$nc_config" --format_display_secrets_in_show_and_select=1)
    rm -rf "$work/src" "$work/dst"
    $CLICKHOUSE_LOCAL --path "$work/src" -n -q "$collection CREATE DATABASE d; SET allow_url_wildcard_from_index_pages = 1;
        CREATE TABLE d.u (x UInt64) ENGINE = URL(nc, format = 'CSV');"
    $CLICKHOUSE_LOCAL "${dump_options[@]}" --path "$work/src" --dump-schema d > "$work/$name.sql" 2>/dev/null
    echo "$name: dump rc=$?, archive gate: $(grep -c '^SET allow_archive_path_syntax = 1;' "$work/$name.sql")"
    $CLICKHOUSE_LOCAL --path "$work/dst" -q "$collection"
    $CLICKHOUSE_LOCAL --path "$work/dst" --allow_archive_path_syntax=0 -n --queries-file "$work/$name.sql" 2>&1 \
        | grep -o 'Code: [0-9]*' | head -1
    echo "$name: replay rc=${PIPESTATUS[0]}"
    local source_definition replayed_definition
    source_definition=$($CLICKHOUSE_LOCAL --path "$work/src" -q "SHOW CREATE TABLE d.u")
    replayed_definition=$($CLICKHOUSE_LOCAL --path "$work/dst" -q "SHOW CREATE TABLE d.u")
    echo "$name: same definition: $([ -n "$source_definition" ] && [ "$source_definition" = "$replayed_definition" ] && echo 1 || echo 0)"
}

named_collection_round_trip collection_archive 'http://127.0.0.1:1/d/*.tar :: data.csv' shown
named_collection_round_trip collection_plain 'http://127.0.0.1:1/d/*.csv' shown
named_collection_round_trip collection_masked 'http://127.0.0.1:1/d/*.csv' masked
