#!/usr/bin/env bash

# --dump-schema cases from review: a view the analyzer inlines into a probed query, a table created AS a table function,
# a quoted named-collection key, a parameterized view called in a Remote target, and table-function structures that are
# computed or carry a NULL modifier.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Creates the objects in a fresh source, dumps them and replays the dump into a fresh target with extra options.
round_trip() {
    local name=$1 ddl=$2
    shift 2
    rm -rf "$work/src" "$work/dst"
    $CLICKHOUSE_LOCAL --path "$work/src" -n -q "$ddl"
    $CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > "$work/$name.sql" 2>/dev/null
    echo "$name: dump rc=$?"
    $CLICKHOUSE_LOCAL --path "$work/dst" "$@" -n --queries-file "$work/$name.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
    echo "$name: replay rc=${PIPESTATUS[0]}"
}

# Under `analyzer_inline_views` the analyzer replaces a view with its query, so the probes of a materialized view that
# reads a view must not run the scalar subquery of that view.
$CLICKHOUSE_CLIENT -n -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.src (x UInt64) ENGINE = Memory;
    CREATE VIEW ${CLICKHOUSE_DATABASE}.v AS SELECT x, (SELECT sum(number) FROM numbers(1234567)) AS s FROM ${CLICKHOUSE_DATABASE}.src;
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.mv ENGINE = Memory AS SELECT x, s FROM ${CLICKHOUSE_DATABASE}.v GROUP BY x, s;"
$CLICKHOUSE_CLIENT --analyzer_inline_views=1 --dump-schema "$CLICKHOUSE_DATABASE" > /dev/null 2>&1
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT 'inlined view scalar subquery runs: ' || toString(count()) FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND query LIKE 'EXPLAIN QUERY TREE%' AND read_rows > 0"

# Creating `aaa_mv` resolves the `loop` of `aaa_loop`, so `aaa_loop` must follow `zzz_src`, which reading `base` puts
# on the dependency level of `aaa_mv`.
round_trip as_table_function "
    CREATE DATABASE d;
    CREATE TABLE d.base (id UInt64) ENGINE = Memory;
    CREATE VIEW d.zzz_src AS SELECT id FROM d.base;
    CREATE TABLE d.aaa_loop (id UInt64) AS loop('d', 'zzz_src');
    CREATE MATERIALIZED VIEW d.aaa_mv ENGINE = Memory AS SELECT id FROM d.aaa_loop;"

# Creating a local Remote table analyzes its target, which calls the parameterized view.
round_trip parameterized_view "
    CREATE DATABASE d;
    CREATE VIEW d.zzz_p AS SELECT {n:UInt64} AS x;
    CREATE TABLE d.aaa_proxy (x UInt64) ENGINE = Remote('127.0.0.1', view(SELECT x FROM d.zzz_p(n = 1)));"

# The only gated type is in a structure that the server folds from strings.
round_trip computed_structure "
    SET allow_suspicious_low_cardinality_types = 1;
    CREATE DATABASE d;
    CREATE TABLE d.t (n UInt64)
    ENGINE = Remote('127.0.0.1', view(SELECT count() AS n FROM format(CSV, concat('x LowCardinality(', 'UInt64)'), '1\n')));"
grep '^SET ' "$work/computed_structure.sql"

# The only gated type is the one the NULL modifier makes, and the target turns its gate off.
round_trip null_modifier "
    CREATE DATABASE d;
    CREATE TABLE d.t (n UInt64)
    ENGINE = Remote('127.0.0.1', view(SELECT count() AS n FROM format(CSV, 'x Tuple(a UInt8) NULL', '(1)\n')));" \
    --enable_nullable_tuple_type=0
grep '^SET ' "$work/null_modifier.sql"

# A named-collection override key may be a string; the collection's own table does not exist.
nc_users="$work/nc_users.xml"
nc_config="$work/nc.xml"
cat > "$nc_users" <<EOF
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
EOF
cat > "$nc_config" <<EOF
<clickhouse>
    <users_config>${nc_users}</users_config>
    <display_secrets_in_show_and_select>1</display_secrets_in_show_and_select>
</clickhouse>
EOF
rm -rf "$work/nc"
$CLICKHOUSE_LOCAL --config-file "$nc_config" --path "$work/nc" -n -q "
    CREATE DATABASE d;
    CREATE NAMED COLLECTION nc_quoted AS host = '127.0.0.1', db = 'd', table = 'missing';
    CREATE TABLE d.zzz_src (id UInt64) ENGINE = Memory;
    CREATE VIEW d.aaa_reader AS SELECT * FROM remote(nc_quoted, 'table' = 'zzz_src');"
$CLICKHOUSE_LOCAL --config-file "$nc_config" --path "$work/nc" --format_display_secrets_in_show_and_select=1 \
    --dump-schema d > "$work/nc.sql" 2>/dev/null
echo "quoted_key: dump rc=$?"
src_line=$(grep -n 'CREATE TABLE d\.zzz_src' "$work/nc.sql" | head -1 | cut -d: -f1)
reader_line=$(grep -n 'CREATE VIEW d\.aaa_reader' "$work/nc.sql" | head -1 | cut -d: -f1)
echo "quoted_key: source dumped first: $([ -n "$src_line" ] && [ -n "$reader_line" ] && [ "$src_line" -lt "$reader_line" ] && echo yes || echo no)"
