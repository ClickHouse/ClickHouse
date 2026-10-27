#!/usr/bin/env bash

# --dump-schema dumps what a replay needs to recreate the schema, without changing data:
# - A refreshable materialized view runs a refresh as soon as CREATE makes it, unless the CREATE says `EMPTY`, which the
#   server does not store. The dump adds `EMPTY` to every form of the view, so the replay writes no rows.
# - A view that runs as a `DEFINER` user needs that user, which is not in any database, so the dump warns about it.
# - A secondary index that the server accepts only on ATTACH, such as one an older version created, fails the replayed
#   CREATE, so the dump refuses its table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

echo '--- a refreshable materialized view is dumped with EMPTY ---'
$CLICKHOUSE_LOCAL --path "$work/refresh_src" -n -q "
CREATE DATABASE d;
CREATE TABLE d.dst_append (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE d.dst_replace (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW d.to_append REFRESH EVERY 1 YEAR APPEND TO d.dst_append EMPTY AS SELECT number AS x FROM numbers(5);
CREATE MATERIALIZED VIEW d.to_replace REFRESH EVERY 1 YEAR TO d.dst_replace EMPTY AS SELECT number AS x FROM numbers(5);
CREATE MATERIALIZED VIEW d.inner_append REFRESH EVERY 1 YEAR APPEND ENGINE = MergeTree ORDER BY x EMPTY
    AS SELECT number AS x FROM numbers(5);
CREATE MATERIALIZED VIEW d.inner_replace REFRESH AFTER 1 YEAR ENGINE = MergeTree ORDER BY x COMMENT 'c' EMPTY
    AS SELECT number AS x FROM numbers(5);
CREATE MATERIALIZED VIEW d.depends REFRESH EVERY 1 YEAR DEPENDS ON d.inner_replace, other.upstream ENGINE = Memory EMPTY
    AS SELECT x FROM d.inner_replace;
"
$CLICKHOUSE_LOCAL --path "$work/refresh_src" --dump-schema d > "$work/refresh.sql" 2> "$work/refresh.err"
echo "dump rc=$?, views dumped with EMPTY: $(grep -c '^CREATE MATERIALIZED VIEW .* EMPTY DEFINER = ' "$work/refresh.sql")"
# `DEPENDS ON` does not order the dump: CREATE accepts a missing view there. One outside the dump is reported.
grep 'Warning' "$work/refresh.err"
# Without `EMPTY` every CREATE starts a refresh, which the replaying session waits for before it counts the rows.
{
    cat "$work/refresh.sql"
    echo "SYSTEM WAIT VIEW d.to_append; SYSTEM WAIT VIEW d.to_replace; SYSTEM WAIT VIEW d.inner_append; SYSTEM WAIT VIEW d.inner_replace;
        SYSTEM WAIT VIEW d.depends;"
    echo "SELECT 'rows written by the replay', (SELECT count() FROM d.dst_append) + (SELECT count() FROM d.dst_replace)
        + (SELECT count() FROM d.inner_append) + (SELECT count() FROM d.inner_replace);"
    echo "SELECT view, status, last_success_time IS NULL FROM system.view_refreshes WHERE database = 'd' ORDER BY view;"
    echo "SELECT name, create_table_query FROM system.tables WHERE database = 'd' AND NOT startsWith(name, '.') ORDER BY name
        INTO OUTFILE '$work/refresh.replayed.tsv' FORMAT TSV;"
} > "$work/refresh.replay.sql"
$CLICKHOUSE_LOCAL --path "$work/refresh_dst" -n --queries-file "$work/refresh.replay.sql"
echo "replay rc=$?"
# The replayed views are as refreshable as the source ones: the server does not store `EMPTY`.
$CLICKHOUSE_LOCAL --path "$work/refresh_src" -q "SELECT name, create_table_query FROM system.tables
    WHERE database = 'd' AND NOT startsWith(name, '.') ORDER BY name FORMAT TSV" > "$work/refresh.source.tsv"
echo "same definitions: $(cmp -s "$work/refresh.source.tsv" "$work/refresh.replayed.tsv" && echo 1 || echo 0)"

echo '--- a view that runs as a definer user ---'
$CLICKHOUSE_LOCAL --path "$work/definer_src" -n -q "
CREATE USER alice;
CREATE DATABASE d;
CREATE TABLE d.t (x UInt64) ENGINE = Memory;
CREATE TABLE d.t2 (x UInt64) ENGINE = Memory;
CREATE VIEW d.v DEFINER = alice SQL SECURITY DEFINER AS SELECT x FROM d.t;
CREATE MATERIALIZED VIEW d.mv TO d.t2 DEFINER = alice SQL SECURITY DEFINER AS SELECT x FROM d.t;
CREATE VIEW d.own DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT x FROM d.t;
CREATE VIEW d.invoker SQL SECURITY INVOKER AS SELECT x FROM d.t;
CREATE VIEW d.plain AS SELECT x FROM d.t;
"
$CLICKHOUSE_LOCAL --path "$work/definer_src" --dump-schema d > "$work/definer.sql" 2> "$work/definer.err"
echo "dump rc=$?"
grep 'Warning' "$work/definer.err"
# The dump creates no user: its replay fails where the definer is missing, and succeeds where it exists.
$CLICKHOUSE_LOCAL --path "$work/definer_dst" -n --queries-file "$work/definer.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
{ echo 'CREATE USER alice;'; cat "$work/definer.sql"; } > "$work/definer.replay.sql"
$CLICKHOUSE_LOCAL --path "$work/definer_dst_with_user" -n --queries-file "$work/definer.replay.sql"
echo "replay with the user rc=$?"

echo '--- a secondary index that only ATTACH accepts ---'
# Attaches `d.t` with the index `$2` in a fresh source, as an upgraded server has it, dumps it, and replays the dump.
attach_only() {
    local name=$1 index=$2
    rm -rf "$work/attach_src" "$work/attach_dst"
    $CLICKHOUSE_LOCAL --path "$work/attach_src" -n -q "CREATE DATABASE d;
        ATTACH TABLE d.t UUID '00000000-0000-0000-0000-000000004836' (a UInt64, s Array(UInt64), $index)
        ENGINE = MergeTree ORDER BY a;"
    $CLICKHOUSE_LOCAL --path "$work/attach_src" --dump-schema d > "$work/attach.sql" 2> "$work/attach.err"
    local dump_rc=$? refusal
    refusal=$(grep -o 'its secondary index [^.]*' "$work/attach.err")
    echo "$name: dump rc=$dump_rc${refusal:+, $refusal}"
    if [ "$dump_rc" = 0 ]; then
        $CLICKHOUSE_LOCAL --path "$work/attach_dst" -n --queries-file "$work/attach.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
        echo "$name: replay rc=${PIPESTATUS[0]}"
    fi
}

attach_only hypothesis 'INDEX i a = 1 TYPE hypothesis GRANULARITY 1'
attach_only rand 'INDEX i a + rand() TYPE minmax GRANULARITY 1'
attach_only lambda 'INDEX i arrayMap(x -> x + rand(), s) TYPE bloom_filter GRANULARITY 1'
attach_only array_join 'INDEX i arrayJoin(s) TYPE minmax GRANULARITY 1'
attach_only bloom_filter_rate 'INDEX i a TYPE bloom_filter(1) GRANULARITY 1'
attach_only bloom_filter_parameters 'INDEX i a TYPE bloom_filter(0.1, 2) GRANULARITY 1'
# Indexes that CREATE accepts too are dumped.
attach_only accepted 'INDEX i a TYPE bloom_filter(0.01) GRANULARITY 1, INDEX j a + 1 TYPE minmax GRANULARITY 1,
    INDEX k arrayMap(x -> x + 1, s) TYPE bloom_filter GRANULARITY 1'
