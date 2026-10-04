#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A hierarchical database name (`a.b`; see 05077_hierarchical_names) is accepted by every statement that takes a
# database name, not only by `CREATE DATABASE`, `DROP DATABASE` and `USE`.

db=$CLICKHOUSE_DATABASE

function run()
{
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 | sed "s/${db}/db/g"
}

run "CREATE DATABASE ${db}.sub"

echo '--- EXISTS DATABASE and SHOW CREATE DATABASE'
run "EXISTS DATABASE ${db}.sub"
run "EXISTS DATABASE ${db}.nothing"
run "SHOW CREATE DATABASE ${db}.sub" | sed 's/ENGINE.*//'

echo '--- RENAME DATABASE'
run "RENAME DATABASE ${db}.sub TO ${db}.sub2"
run "EXISTS DATABASE ${db}.sub"
run "EXISTS DATABASE ${db}.sub2"
run "SELECT name FROM system.databases WHERE name LIKE '${db}.%' ORDER BY name"

echo '--- SYSTEM RESTORE DATABASE REPLICA names the database as written'
run "SYSTEM RESTORE DATABASE REPLICA ${db}.sub2" | grep -o "Database db.sub2 is not Replicated" | sort -u

echo '--- ALTER DATABASE'
run "ALTER DATABASE ${db}.sub2 MODIFY COMMENT 'a hierarchical name'"
run "SELECT comment FROM system.databases WHERE name = '${db}.sub2'"

run "DROP DATABASE ${db}.sub2"
run "EXISTS DATABASE ${db}.sub2"
