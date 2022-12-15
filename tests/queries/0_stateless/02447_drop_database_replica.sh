#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db="rdb_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "system flush logs"
$CLICKHOUSE_CLIENT --allow_experimental_database_replicated=1 -q "create database $db engine=Replicated('/test/$CLICKHOUSE_DATABASE/rdb', 's1', 'r1')"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "create table $db.t as system.query_log"   # Suppress style check: current_database=$CLICKHOUSE_DATABASE
$CLICKHOUSE_CLIENT -q "show tables from $db"

$CLICKHOUSE_CLIENT -q "system drop database replica 's1|r1' from table t" 2>&1| grep -Fac "SYNTAX_ERROR"
$CLICKHOUSE_CLIENT -q "system drop database replica 's1|r1' from database $db" 2>&1| grep -Fac "There is a local database"
$CLICKHOUSE_CLIENT -q "system drop database replica 's1|r1' from zkpath '/test/$CLICKHOUSE_DATABASE/rdb'" 2>&1| grep -Fac "There is a local database"
$CLICKHOUSE_CLIENT -q "system drop database replica 's1|r1' from zkpath '/test/$CLICKHOUSE_DATABASE/rdb/'" 2>&1| grep -Fac "There is a local database"

$CLICKHOUSE_CLIENT -q "system drop database replica 's1|r1' from zkpath '/test/$CLICKHOUSE_DATABASE/'" 2>&1| grep -Fac "does not look like a path of Replicated database"
$CLICKHOUSE_CLIENT -q "system drop database replica 's2|r1' from zkpath '/test/$CLICKHOUSE_DATABASE/rdb'" 2>&1| grep -Fac "does not exist"
$CLICKHOUSE_CLIENT -q "system drop database replica 's2/r1' from zkpath '/test/$CLICKHOUSE_DATABASE/rdb'" 2>&1| grep -Fac "Invalid replica name"

db2="${db}_2"
$CLICKHOUSE_CLIENT --allow_experimental_database_replicated=1 -q "create database $db2 engine=Replicated('/test/$CLICKHOUSE_DATABASE/rdb', 's1', 'r2')"
$CLICKHOUSE_CLIENT -q "system sync database replica $db"
$CLICKHOUSE_CLIENT -q "select cluster, shard_num, replica_num from system.clusters where cluster='$db' order by shard_num, replica_num"
$CLICKHOUSE_CLIENT -q "system drop database replica 's1|r1' from database $db2" 2>&1| grep -Fac "is active, cannot drop it"

$CLICKHOUSE_CLIENT -q "detach database $db2"
$CLICKHOUSE_CLIENT -q "system drop database replica 's1|r2' from database $db"
$CLICKHOUSE_CLIENT -q "attach database $db2" 2>/dev/null
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "create table $db2.t2 as system.query_log" 2>&1| grep -Fac "Database is in readonly mode"   # Suppress style check: current_database=$CLICKHOUSE_DATABASE

$CLICKHOUSE_CLIENT -q "detach database $db"
$CLICKHOUSE_CLIENT -q "system drop database replica 's1|r1' from zkpath '/test/$CLICKHOUSE_DATABASE/rdb/'"
$CLICKHOUSE_CLIENT -q "attach database $db" 2>/dev/null
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "create table $db.t2 as system.query_log" 2>&1| grep -Fac "Database is in readonly mode"   # Suppress style check: current_database=$CLICKHOUSE_DATABASE
$CLICKHOUSE_CLIENT -q "show tables from $db"

db3="${db}_3"
$CLICKHOUSE_CLIENT --allow_experimental_database_replicated=1 -q "create database $db3 engine=Replicated('/test/$CLICKHOUSE_DATABASE/rdb', 's1', 'r1')"
$CLICKHOUSE_CLIENT -q "system sync database replica $db3"
$CLICKHOUSE_CLIENT -q "select cluster, shard_num, replica_num from system.clusters where cluster='$db3'"

$CLICKHOUSE_CLIENT -q "drop database $db"
$CLICKHOUSE_CLIENT -q "drop database $db2"
$CLICKHOUSE_CLIENT -q "drop database $db3"
