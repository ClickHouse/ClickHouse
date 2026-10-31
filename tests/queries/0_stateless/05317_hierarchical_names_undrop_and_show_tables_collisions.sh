#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Tag no-ordinary-database: UNDROP needs an Atomic database.
# Tag no-replicated-database: Replicated database does not support UNDROP.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (see 05077_hierarchical_names) in `SHOW TABLES` when several tables have the same relative name,
# and in `UNDROP TABLE`.

db=$CLICKHOUSE_DATABASE

function run()
{
    $CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously 0 -q "$1" 2>&1 | sed "s/${db}/db/g"
}

run "CREATE DATABASE ${db}.ns"
run "CREATE DATABASE ${db}.ns.c"

echo '--- SHOW TABLES lists a relative name once, for the table that the name written as one identifier resolves to'
run "CREATE TABLE ${db}.ns.t (x String) ENGINE = Memory"
run "CREATE TABLE ${db}.\"ns.t\" (x String) ENGINE = Memory"
run "CREATE TABLE ${db}.ns.\"c.t\" (x String) ENGINE = Memory"
run "CREATE TABLE ${db}.ns.c.t (x String) ENGINE = Memory"
run "CREATE TABLE ${db}.\"ns.u\" (x String) ENGINE = MergeTree ORDER BY x"
run "INSERT INTO \`${db}.ns\`.\`t\` VALUES ('db.ns . t')"
run "INSERT INTO \`${db}\`.\`ns.t\` VALUES ('db . ns.t')"
run "INSERT INTO \`${db}.ns\`.\`c.t\` VALUES ('db.ns . c.t')"
run "INSERT INTO \`${db}.ns.c\`.\`t\` VALUES ('db.ns.c . t')"
$CLICKHOUSE_CLIENT -n -q "
USE ${db}.ns;
SHOW TABLES;
SELECT * FROM t;
SELECT * FROM \"c.t\";
SHOW TABLES LIKE 'c.%';
" 2>&1 | sed "s/${db}/db/g"

echo '--- UNDROP TABLE restores the dropped table that the name denotes'
run "DROP TABLE \`${db}\`.\`ns.u\`"
run "UNDROP TABLE ${db}.ns.u"
run "EXISTS TABLE \`${db}\`.\`ns.u\`"
run "DROP TABLE \`${db}\`.\`ns.u\`"
$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously 0 -n -q "
USE ${db}.ns;
UNDROP TABLE u;
" 2>&1 | sed "s/${db}/db/g"
run "EXISTS TABLE \`${db}\`.\`ns.u\`"

run "DROP DATABASE ${db}.ns.c"
run "DROP DATABASE ${db}.ns"
