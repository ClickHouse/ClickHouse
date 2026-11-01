#!/usr/bin/env bash

# --dump-schema cases from review: a view the analyzer inlines into a probed query, a table created AS a table function,
# a parameterized view, named-collection override keys written as strings, and structures of the table function that a
# local Remote table reads.

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
    grep '^SET ' "$work/$name.sql"
    $CLICKHOUSE_LOCAL --path "$work/dst" "$@" -n --queries-file "$work/$name.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
    echo "$name: replay rc=${PIPESTATUS[0]}"
}

# Prints whether the dump `$1` creates `$2` before `$3`.
dumped_before() {
    local first second
    first=$(grep -n "^CREATE [A-Z ]*$2 " "$1" | head -1 | cut -d: -f1)
    second=$(grep -n "^CREATE [A-Z ]*$3 " "$1" | head -1 | cut -d: -f1)
    [ -n "$first" ] && [ -n "$second" ] && [ "$first" -lt "$second" ] && echo yes || echo no
}

# Under `analyzer_inline_views` the analyzer replaces a view with its query, so the probes of a materialized view that
# reads a view must not run the scalar subquery of that view. Each probe reads only the rows of its own result.
$CLICKHOUSE_CLIENT -n -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.src (x UInt64) ENGINE = Memory;
    CREATE VIEW ${CLICKHOUSE_DATABASE}.v AS SELECT x, (SELECT sum(number) FROM numbers(1234567)) AS s FROM ${CLICKHOUSE_DATABASE}.src;
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.mv ENGINE = Memory AS SELECT x, s FROM ${CLICKHOUSE_DATABASE}.v GROUP BY x, s;"
$CLICKHOUSE_CLIENT --analyzer_inline_views=1 --dump-schema "$CLICKHOUSE_DATABASE" > /dev/null 2>&1
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT 'probes: ' || toString(count() > 0) || ', probes that ran the scalar subquery: ' || toString(countIf(read_rows > result_rows))
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND query LIKE 'EXPLAIN QUERY TREE%'"

# Creating `aaa_mv` resolves the `loop` of `aaa_loop`, so `aaa_loop` must follow `zzz_src`, which reading `base` puts
# on the dependency level of `aaa_mv`.
round_trip as_table_function "
    CREATE DATABASE d;
    CREATE TABLE d.base (id UInt64) ENGINE = Memory;
    CREATE VIEW d.zzz_src AS SELECT id FROM d.base;
    CREATE TABLE d.aaa_loop (id UInt64) AS loop('d', 'zzz_src');
    CREATE MATERIALIZED VIEW d.aaa_mv ENGINE = Memory AS SELECT id FROM d.aaa_loop;"
echo "as_table_function: source dumped first: $(dumped_before "$work/as_table_function.sql" d.zzz_src d.aaa_loop)"

# A parameterized view is called like a table function; reading `base` puts it on the dependency level of `aaa_mv`.
round_trip parameterized_view "
    CREATE DATABASE d;
    CREATE TABLE d.base (x UInt64) ENGINE = Memory;
    CREATE TABLE d.b_src (x UInt64) ENGINE = Memory;
    CREATE VIEW d.zzz_p AS SELECT x FROM d.base WHERE x = {n:UInt64};
    CREATE MATERIALIZED VIEW d.aaa_mv ENGINE = Memory AS SELECT s.x AS x FROM d.b_src AS s INNER JOIN d.zzz_p(n = 1) AS p ON s.x = p.x;"
echo "parameterized_view: view dumped first: $(dumped_before "$work/parameterized_view.sql" d.zzz_p d.aaa_mv)"

# Creating a local Remote table resolves its table-function target, which validates the types of its structure.
round_trip remote_structure "
    SET allow_suspicious_low_cardinality_types = 1;
    CREATE DATABASE d;
    CREATE TABLE d.t (x UInt64) ENGINE = Remote('127.0.0.1', format(CSV, 'x LowCardinality(UInt64)', '1\n'));"

# The only gated type is the one the NULL modifier makes, and the target turns its gate off.
round_trip null_modifier "
    CREATE DATABASE d;
    CREATE TABLE d.t (n UInt64) ENGINE = Remote('127.0.0.1', format(CSV, 'x Tuple(a UInt8) NULL', '(1)\n'));" \
    --enable_nullable_tuple_type=0

# A table engine reads a named-collection override key written as a string, while a table function in a query does not:
# the analyzer folds that argument into a constant first. `aab_target` and `aad_own` are not what is read.
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
    CREATE TABLE d.base (id UInt64) ENGINE = Memory;
    CREATE TABLE d.aab_target (id UInt64) ENGINE = Memory;
    CREATE TABLE d.aad_own (id UInt64) ENGINE = Memory;
    CREATE VIEW d.zzz_own AS SELECT id FROM d.base;
    CREATE VIEW d.zzz_target AS SELECT id FROM d.base;
    CREATE NAMED COLLECTION nc_query AS host = '127.0.0.1', db = 'd', table = 'zzz_own';
    CREATE NAMED COLLECTION nc_engine AS host = '127.0.0.1', db = 'd', table = 'aad_own';
    CREATE VIEW d.aaa_query AS SELECT * FROM remote(nc_query, 'table' = 'aab_target');
    CREATE TABLE d.aac_engine (id UInt64) ENGINE = Remote(nc_engine, 'table' = 'zzz_target');"
$CLICKHOUSE_LOCAL --config-file "$nc_config" --path "$work/nc" --format_display_secrets_in_show_and_select=1 \
    --dump-schema d > "$work/nc.sql" 2>/dev/null
echo "string_keys: dump rc=$?"
echo "string_keys: query after the collection's table: $(dumped_before "$work/nc.sql" d.zzz_own d.aaa_query)"
echo "string_keys: engine after the overriding table: $(dumped_before "$work/nc.sql" d.zzz_target d.aac_engine)"
