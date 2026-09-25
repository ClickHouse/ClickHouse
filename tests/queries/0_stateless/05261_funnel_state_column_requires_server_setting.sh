#!/usr/bin/env bash
# A column whose type stores a sequenceNextNode state may only be declared when
# enable_funnel_functions is enabled for the server, and such a column is readable on a later run.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

root="$CLICKHOUSE_TMP/05261_funnel_state"
rm -rf "${root:?}"
mkdir -p "$root"

state="AggregateFunction(sequenceNextNode('forward', 'head'), DateTime, Nullable(String), UInt8)"
select_state="SELECT sequenceNextNodeState('forward', 'head')(now(), 'a', 1) AS c"
native="$root/state.native"
$CLICKHOUSE_LOCAL --enable_funnel_functions=1 --query "$select_state FORMAT Native" > "$native"
case_number=0

# The command line sets the value for the server, a SET inside the query text only for the session.
# Every case gets its own directory, because a table that cannot load stops each later run on the
# same path.
new_path() {
    case_number=$((case_number + 1))
    path="$root/$case_number"
    mkdir -p "$path"
}

# $1 names the surface, $2 prepares it with the value set for the server, $3 is the statement under
# test. Running $3 both ways is what makes the refusal attributable to this setting: a statement that
# fails for any other reason fails in the first run too, whose output is not filtered.
refused() {
    new_path
    [ -z "$2" ] || $CLICKHOUSE_LOCAL --path "$path" --enable_funnel_functions=1 --query "$2"
    $CLICKHOUSE_LOCAL --path "$path" --enable_funnel_functions=1 --query "$3"
    echo "$1: accepted for the server"

    new_path
    [ -z "$2" ] || $CLICKHOUSE_LOCAL --path "$path" --enable_funnel_functions=1 --query "$2"
    echo -n "$1: "
    $CLICKHOUSE_LOCAL --path "$path" --query "SET enable_funnel_functions = 1; $3" 2>&1 \
        | grep -om1 'setting for the server to enable it'
}

# An object that no later run rebuilds is exempt, so the value set only for the session is enough.
allowed() {
    new_path
    [ -z "$2" ] || $CLICKHOUSE_LOCAL --path "$path" --enable_funnel_functions=1 --query "$2"
    echo -n "$1: "
    $CLICKHOUSE_LOCAL --path "$path" --query "SET enable_funnel_functions = 1; $3"
}

new_path
$CLICKHOUSE_LOCAL --path "$path" --enable_funnel_functions=1 --query "CREATE TABLE t (c $state) ENGINE = Memory"
echo -n "read back: "
$CLICKHOUSE_LOCAL --path "$path" --enable_funnel_functions=1 --query "SELECT count() FROM t"
echo -n "read back with the server value off: "
$CLICKHOUSE_LOCAL --path "$path" --query "SELECT count() FROM t" 2>&1 \
    | grep -om1 'Set .enable_funnel_functions. setting to enable it'

refused "column" "" "CREATE TABLE t (c $state) ENGINE = Memory"
refused "add column" "CREATE TABLE m (x UInt64) ENGINE = MergeTree ORDER BY tuple()" "ALTER TABLE m ADD COLUMN c $state"
refused "modify column" "CREATE TABLE m (x String) ENGINE = MergeTree ORDER BY tuple()" "ALTER TABLE m MODIFY COLUMN x $state"
refused "inferred column" "" "CREATE TABLE u ENGINE = Memory AS $select_state"
refused "view" "" "CREATE VIEW v AS $select_state"
refused "materialized view query" "CREATE TABLE q (c UInt64) ENGINE = MergeTree ORDER BY tuple(); CREATE MATERIALIZED VIEW mv TO q AS SELECT 1::UInt64 AS c" "ALTER TABLE mv MODIFY QUERY $select_state"
refused "state argument" "" "CREATE TABLE n (c AggregateFunction(any, $state)) ENGINE = Memory"
refused "array element" "" "CREATE TABLE a (c Array($state)) ENGINE = Memory"
refused "tuple element" "" "CREATE TABLE p (c AggregateFunction(sequenceNextNodeTuple('forward', 'head'), Tuple(DateTime, DateTime), Tuple(Nullable(String), Nullable(String)), Tuple(UInt8, UInt8))) ENGINE = Memory"
refused "schema read by the engine" "" "CREATE TABLE f ENGINE = File(Native, '$native')"
refused "attach with a full definition" "" "ATTACH TABLE t UUID 'c0ffee00-0526-4100-8000-000000000001' (c $state) ENGINE = Memory"

allowed "temporary table" "" "CREATE TEMPORARY TABLE tt (c $state) ENGINE = Memory; SELECT count() FROM tt"
allowed "table in a Memory database" "CREATE DATABASE mem ENGINE = Memory" "CREATE TABLE mem.t (c $state) ENGINE = Memory; SELECT count() FROM mem.t"

# Restoring a backup brings back a definition that was already accepted, so the value set only for
# the session is enough.
mkdir -p "$root/backups"
config="$root/config.xml"
cat > "$config" <<EOF
<clickhouse>
    <backups>
        <allowed_path>$root/backups</allowed_path>
    </backups>
</clickhouse>
EOF
new_path
$CLICKHOUSE_LOCAL --config-file "$config" --path "$path" --enable_funnel_functions=1 --query "
    CREATE TABLE t (c $state) ENGINE = MergeTree ORDER BY tuple();
    BACKUP TABLE t TO File('$root/backups/b1') FORMAT Null"
new_path
$CLICKHOUSE_LOCAL --config-file "$config" --path "$path" --query "
    SET enable_funnel_functions = 1;
    RESTORE TABLE default.t FROM File('$root/backups/b1') FORMAT Null"
echo -n "restore from a backup: "
$CLICKHOUSE_LOCAL --config-file "$config" --path "$path" --enable_funnel_functions=1 --query "SELECT count() FROM t"

rm -rf "${root:?}"
