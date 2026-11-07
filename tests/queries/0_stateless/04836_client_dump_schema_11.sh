#!/usr/bin/env bash
# Tags: long, zookeeper, no-fasttest
# Tag zookeeper: one case dumps a `Replicated` database.

# --dump-schema cases from review: nullability, nested table functions, sorting keys, bad selects, Alias and Overlay
# order, a Replicated database's collection, and scalar subqueries a dump must not run.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# clickhouse-local refuses --dump-schema with a file, pipe or socket on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Creates the objects in a fresh source, dumps them and replays the dump into a fresh target with extra options.
round_trip() {
    local name=$1 ddl=$2 dbs=$3
    shift 3
    rm -rf "$work/src" "$work/dst"
    $CLICKHOUSE_LOCAL --path "$work/src" -n -q "$ddl"
    $CLICKHOUSE_LOCAL --path "$work/src" --dump-schema "$dbs" > "$work/$name.sql" 2>/dev/null
    echo "$name: dump rc=$?"
    grep '^SET ' "$work/$name.sql"
    $CLICKHOUSE_LOCAL --path "$work/dst" "$@" -n --queries-file "$work/$name.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
    echo "$name: replay rc=${PIPESTATUS[0]}"
}

round_trip default_nullable "
    CREATE DATABASE d;
    CREATE TABLE d.t (x UInt8) ENGINE = MergeTree ORDER BY x;" d --data_type_default_nullable=1
$CLICKHOUSE_LOCAL --path "$work/dst" -q "SELECT type FROM system.columns WHERE database = 'd' AND table = 't'"

round_trip nested_table_function "
    CREATE DATABASE d;
    CREATE TABLE d.src (x UInt64) ENGINE = Memory;
    CREATE VIEW d.v AS SELECT * FROM remote('127.0.0.1', view(SELECT x, rand() AS r FROM d.src));" d

round_trip key_tuple_element "
    SET allow_suspicious_primary_key = 1;
    CREATE DATABASE d;
    CREATE TABLE d.t (v Tuple(a SimpleAggregateFunction(sum, UInt64))) ENGINE = MergeTree ORDER BY tupleElement(v, 'a');" d

round_trip key_cast "
    SET allow_suspicious_primary_key = 1;
    CREATE DATABASE d;
    CREATE TABLE d.t (x UInt64) ENGINE = MergeTree ORDER BY CAST(x, 'SimpleAggregateFunction(sum, UInt64)');" d

round_trip bad_select "
    SET allow_materialized_view_with_bad_select = 1;
    CREATE DATABASE d;
    CREATE TABLE d.src (x UInt64) ENGINE = Memory;
    CREATE TABLE d.dst (x UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW d.mv TO d.dst (x UInt64) AS SELECT missing_column AS x FROM d.src;" d

# The alias sorts before its target, which a view makes a level deeper; the materialized view reads through the alias.
round_trip alias "
    CREATE DATABASE d;
    CREATE TABLE d.zzz_base (x UInt8) ENGINE = Memory;
    CREATE VIEW d.zzz_src AS SELECT * FROM d.zzz_base;
    CREATE TABLE d.aaa_alias ENGINE = Alias('d', 'zzz_src');
    CREATE TABLE d.aaa_dst (x UInt8) ENGINE = Memory;
    CREATE MATERIALIZED VIEW d.aaa_mv TO d.aaa_dst AS SELECT x FROM d.aaa_alias;" d

# The overlay database sorts before its source database.
overlay_ddl="
    CREATE DATABASE z_src;
    CREATE TABLE z_src.t (x UInt8) ENGINE = Memory;
    CREATE DATABASE a_overlay ENGINE = Overlay('z_src');
    CREATE DATABASE a_reader;
    CREATE VIEW a_reader.v AS SELECT * FROM a_overlay.t;"
round_trip overlay "$overlay_ddl" a_overlay,a_reader,z_src
rm -rf "$work/dst" "$work/dir"
$CLICKHOUSE_LOCAL --path "$work/src" --dump-schema a_overlay,a_reader,z_src --dump-schema-dir "$work/dir" > "$work/dir.out" 2>&1
echo "overlay dir: dump rc=$?"
for db in $(grep -o 'Dumped database [^ ]*' "$work/dir.out" | cut -d' ' -f3); do
    $CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/dir/$db.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
done
echo "overlay dir: replayed $($CLICKHOUSE_LOCAL --path "$work/dst" -q "SELECT count() FROM system.tables WHERE database IN ('a_reader', 'z_src')") tables"

# The database setting `collection_name` names a collection the database reads when it is created.
RDB="rdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
COLL="coll_${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION IF NOT EXISTS $COLL AS cluster_secret = 'secret_${CLICKHOUSE_TEST_UNIQUE_NAME}'"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $RDB ENGINE = Replicated('/test/${CLICKHOUSE_TEST_UNIQUE_NAME}/rdb', 's1', 'r1') SETTINGS collection_name = '$COLL'"
$CLICKHOUSE_CLIENT --dump-schema "$RDB" 2>&1 >/dev/null | grep -c "named collection $COLL"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $RDB SYNC"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS $COLL"

# A dump analyzes stored queries without running their scalar subqueries.
$CLICKHOUSE_CLIENT -n -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.src (x UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.mv ENGINE = Memory
    AS SELECT x, (SELECT sum(number) FROM numbers(1234567)) AS s FROM ${CLICKHOUSE_DATABASE}.src GROUP BY x;"
$CLICKHOUSE_CLIENT --dump-schema "$CLICKHOUSE_DATABASE" > /dev/null 2>&1
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT 'scalar subquery runs: ' || toString(count()) FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND query LIKE 'EXPLAIN QUERY TREE%'
        AND query LIKE '%${CLICKHOUSE_DATABASE}.src%' AND read_rows > 0"
