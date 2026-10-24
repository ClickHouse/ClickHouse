#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (see 05077_hierarchical_names): inside `USE db`, the qualified names `ns.j` of `joinGet` and `ns.d`
# of `dictGet` (in the identifier and the string forms) are bound to `db`.`ns.j` and `db`.`ns.d`, in a stored view and
# in the query sent to the shards. `SHOW TABLES` of a name prefix lists the tables of a remote database under it.

db=$CLICKHOUSE_DATABASE

function run()
{
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 | sed "s/${db}/db/g"
}

run "CREATE TABLE \"ns.j\" (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO \"ns.j\" VALUES (1, 'j1')"
run "CREATE TABLE src (k UInt64, v String) ENGINE = MergeTree ORDER BY k"
run "INSERT INTO src VALUES (1, 'd1')"
run "CREATE DICTIONARY \"ns.d\" (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src' DB '${db}')) LAYOUT(FLAT()) LIFETIME(0)"

echo '--- a view'
run "CREATE VIEW v AS SELECT joinGet(ns.j, 'v', toUInt64(1)) AS a, joinGet('ns.j', 'v', toUInt64(1)) AS b, dictGet(ns.d, 'v', toUInt64(1)) AS c, dictGet('ns.d', 'v', toUInt64(1)) AS d"
run "SELECT replaceRegexpOne(create_table_query, '.* AS SELECT ', '') FROM system.tables WHERE database = currentDatabase() AND name = 'v'"
run "SELECT * FROM v"

echo '--- the query sent to the shards'
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_$RANDOM$RANDOM"
# The explicit aliases keep the names as written out of the column names of the query sent to the shards.
$CLICKHOUSE_CLIENT --query_id "$query_id" --prefer_localhost_replica 0 -q "
    SELECT joinGet(ns.j, 'v', toUInt64(dummy + 1)) AS a, joinGet('ns.j', 'v', toUInt64(dummy + 1)) AS b,
        dictGet(ns.d, 'v', toUInt64(dummy + 1)) AS c, dictGet('ns.d', 'v', toUInt64(dummy + 1)) AS d
    FROM remote('127.0.0.2', system.one)"
run "SYSTEM FLUSH LOGS query_log"
# The queries on the shards may be logged with `current_database` = `default` rather than the test database.
run "SELECT arraySort(arrayDistinct(extractAll(query, '(?:joinGet|dictGet)\\\\(\\'[^\\']+\\''))) AS names
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database IN ['default', currentDatabase()] AND initial_query_id = '$query_id' AND NOT is_initial_query AND type = 'QueryFinish' AND notEmpty(names)"

echo '--- the bound names keep naming the same objects when a database db.ns appears'
run "CREATE DATABASE \`${db}.ns\`"
run "CREATE TABLE \`${db}.ns\`.j (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO \`${db}.ns\`.j VALUES (1, 'other')"
run "CREATE TABLE \`${db}.ns\`.src (k UInt64, v String) ENGINE = Memory"
run "INSERT INTO \`${db}.ns\`.src VALUES (1, 'other')"
run "CREATE DICTIONARY \`${db}.ns\`.d (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src' DB '${db}.ns')) LAYOUT(FLAT()) LIFETIME(0)"
run "SELECT * FROM v"
run "DROP DATABASE \`${db}.ns\`"

echo '--- SHOW TABLES of a prefix of a remote database'
run "CREATE DATABASE \`${db}_pfx.r\` ENGINE = Remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', '${db}')"
run "SHOW TABLES FROM ${db}_pfx"
run "DROP DATABASE \`${db}_pfx.r\`"
