#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Tag no-ordinary-database: UNDROP needs an Atomic database.
# Tag no-replicated-database: Replicated database does not support UNDROP.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (see 05077_hierarchical_names): inside `USE db`, a two-part name `ns.t` without dots in its parts
# is also `db`.`ns.t`, in the target of a view and in `UNDROP TABLE`.

db=$CLICKHOUSE_DATABASE

function run()
{
    $CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously 0 -q "$1" 2>&1 | sed "s/${db}/db/g"
}

run "CREATE TABLE src (x String) ENGINE = Memory"
run "CREATE TABLE \"ns.t\" (x String) ENGINE = MergeTree ORDER BY x"

echo '--- TO ns.t of a materialized view is db.`ns.t`'
run "CREATE MATERIALIZED VIEW mv TO ns.t AS SELECT x FROM src"
run "SELECT replaceRegexpOne(create_table_query, '.* TO ([^ ]+) .*', '\\\\1') FROM system.tables WHERE database = currentDatabase() AND name = 'mv'"
run "INSERT INTO src VALUES ('a')"
run "SELECT * FROM \`${db}\`.\`ns.t\`"
run "DROP TABLE mv"

echo '--- UNDROP TABLE ns.t restores db.`ns.t`'
run "DROP TABLE \`${db}\`.\`ns.t\`"
run "UNDROP TABLE ns.t"
run "SELECT * FROM \`${db}\`.\`ns.t\`"
