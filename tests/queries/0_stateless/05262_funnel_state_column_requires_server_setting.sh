#!/usr/bin/env bash
# A column whose type stores a sequenceNextNode state may only be declared or restored when
# enable_funnel_functions is enabled for the server, and such a column is readable on a later run.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

root="$CLICKHOUSE_TMP/05262_funnel_state"
rm -rf "${root:?}"
mkdir -p "$root/backups"

state="AggregateFunction(sequenceNextNode('forward', 'head'), DateTime, Nullable(String), UInt8)"
select_state="SELECT sequenceNextNodeState('forward', 'head')(now(), 'a', 1) AS c"
native="$root/state.native"
$CLICKHOUSE_LOCAL --enable_funnel_functions=1 --query "$select_state FORMAT Native" > "$native"

config="$root/config.xml"
cat > "$config" <<EOF
<clickhouse>
    <backups>
        <allowed_path>$root/backups</allowed_path>
    </backups>
    <zookeeper>
        <implementation>testkeeper</implementation>
    </zookeeper>
</clickhouse>
EOF

# The command line sets the value for the server, a SET inside the query text only for the session.
# Every case runs in two runs, each on its own path: with the value set for the server, where it must
# be accepted, and with it set only for the session, where it must be refused. A statement that fails
# for any other reason fails in the first run too.
for_server="CREATE DATABASE mem ENGINE = Memory;"
for_session="SET enable_funnel_functions = 1; CREATE DATABASE mem ENGINE = Memory;"
refusals=0

# Prints the refusal message if the latest refusal is the one counted as $2, that is, if every
# statement expected to be refused so far was refused exactly once.
refusal() {
    echo "SELECT '$1: ' || extract(last_error_message, 'setting for the server to enable it') FROM system.errors
        WHERE name = 'UNKNOWN_AGGREGATE_FUNCTION' AND value = $2;"
}

# $1 names the case, $2 prepares it, $3 is the statement under test.
refused() {
    refusals=$((refusals + 1))
    for_server+="
        $2
        $3; SELECT '$1: accepted for the server';"
    for_session+="
        $2
        $3; -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
        $(refusal "$1" "$refusals")"
}

refused "column" "" "CREATE TABLE t (c $state) ENGINE = Memory"
refused "add column" "CREATE TABLE m1 (x UInt64) ENGINE = MergeTree ORDER BY tuple();" "ALTER TABLE m1 ADD COLUMN c $state"
refused "modify column" "CREATE TABLE m2 (x String) ENGINE = MergeTree ORDER BY tuple();" "ALTER TABLE m2 MODIFY COLUMN x $state"
refused "alter through an alias" "CREATE TABLE m3 (x UInt64) ENGINE = MergeTree ORDER BY tuple(); CREATE TABLE mem.a ENGINE = Alias(default, m3);" "ALTER TABLE mem.a ADD COLUMN c $state"
refused "inferred column" "" "CREATE TABLE u ENGINE = Memory AS $select_state"
refused "view" "" "CREATE VIEW v AS $select_state"
refused "materialized view query" "CREATE TABLE q (c UInt64) ENGINE = MergeTree ORDER BY tuple(); CREATE MATERIALIZED VIEW mv TO q AS SELECT 1::UInt64 AS c;" "ALTER TABLE mv MODIFY QUERY $select_state"
refused "state argument" "" "CREATE TABLE n (c AggregateFunction(any, $state)) ENGINE = Memory"
refused "array element" "" "CREATE TABLE a (c Array($state)) ENGINE = Memory"
refused "tuple element" "" "CREATE TABLE p (c AggregateFunction(sequenceNextNodeTuple('forward', 'head'), Tuple(DateTime, DateTime), Tuple(Nullable(String), Nullable(String)), Tuple(UInt8, UInt8))) ENGINE = Memory"
refused "schema read by the engine" "" "CREATE TABLE f ENGINE = File(Native, '$native')"
refused "attach with a full definition" "" "ATTACH TABLE ta UUID 'c0ffee00-0526-4100-8000-000000000001' (c $state) ENGINE = Memory"
refused "attach through a table function" "" "ATTACH TABLE g UUID 'c0ffee00-0526-4100-8000-000000000002' (c $state) AS file('$native', Native)"
refused "attach with a schema read by the engine" "" "ATTACH TABLE h UUID 'c0ffee00-0526-4100-8000-000000000003' ENGINE = File(Native, '$native')"

# An object that no later run rebuilds is exempt, so the value set only for the session is enough.
for_session+="
    CREATE TEMPORARY TABLE tt (c $state) ENGINE = Memory; SELECT 'temporary table: ' || toString(count()) FROM tt;
    CREATE TABLE mem.t (c $state) ENGINE = Memory; SELECT 'table in a Memory database: ' || toString(count()) FROM mem.t;"

# A restored table is rebuilt on this server, so restoring one needs the value set for the server.
for_server+="
    CREATE TABLE b (c $state) ENGINE = MergeTree ORDER BY tuple();
    BACKUP TABLE b TO File('$root/backups/b') FORMAT Null;
    RESTORE TABLE b AS r FROM File('$root/backups/b') FORMAT Null;"
refusals=$((refusals + 1))
for_session+="
    RESTORE TABLE b FROM File('$root/backups/b') FORMAT Null; -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
    $(refusal "restore from a backup" "$refusals")"

# A refused CREATE is dropped, so a replica refused here leaves nothing behind in Keeper and can be
# created again.
refusals=$((refusals + 1))
for_session+="
    CREATE TABLE mem.r1 (c $state) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', 'r1') ORDER BY tuple();
    CREATE TABLE r2 ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', 'r2') ORDER BY tuple(); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
    $(refusal "new replica with columns from Keeper" "$refusals")
    CREATE TABLE mem.r2 ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', 'r2') ORDER BY tuple();
    SELECT 'created again: ' || toString(count()) FROM mem.r2;"

$CLICKHOUSE_LOCAL --config-file "$config" --path "$root/server" --enable_funnel_functions=1 --query "$for_server"
$CLICKHOUSE_LOCAL --path "$root/server" --enable_funnel_functions=1 --query "
    SELECT 'read back: ' || toString(count()) FROM t;
    SELECT 'restore from a backup: ' || toString(count()) FROM r;"
echo -n "read back with the server value off: "
$CLICKHOUSE_LOCAL --path "$root/server" --query "SELECT count() FROM t" 2>&1 \
    | grep -om1 'Set .enable_funnel_functions. setting to enable it'

$CLICKHOUSE_LOCAL --config-file "$config" --path "$root/session" --query "$for_session"

rm -rf "${root:?}"
