#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db=$CLICKHOUSE_DATABASE
if [[ $($CLICKHOUSE_CLIENT -q "SELECT engine = 'Replicated' FROM system.databases WHERE name='$CLICKHOUSE_DATABASE'") != 1 ]]; then
  $CLICKHOUSE_CLIENT -q "CREATE DATABASE rdb_$CLICKHOUSE_DATABASE ENGINE=Replicated('/test/$CLICKHOUSE_DATABASE/rdb', '1', '1')"
  db="rdb_$CLICKHOUSE_DATABASE"
fi

$CLICKHOUSE_CLIENT --database_replicated_allow_explicit_uuid=0 -q "CREATE TABLE $db.m0
UUID '02858000-1000-4000-8000-000000000000' (n int) ENGINE=Memory" 2>&1| grep -Fac "database_replicated_allow_explicit_uuid"

$CLICKHOUSE_CLIENT --database_replicated_allow_explicit_uuid=1 -q "CREATE TABLE $db.m1
UUID '02858000-1000-4000-8000-0000000000$(($RANDOM % 10))$(($RANDOM % 10))' (n int) ENGINE=Memory"

$CLICKHOUSE_CLIENT --database_replicated_allow_explicit_uuid=2 -q "CREATE TABLE $db.m2
UUID '02858000-1000-4000-8000-000000000002' (n int) ENGINE=Memory"


$CLICKHOUSE_CLIENT --database_replicated_allow_replicated_engine_arguments=0 -q "CREATE TABLE $db.rmt0 (n int)
ENGINE=ReplicatedMergeTree('/test/$CLICKHOUSE_DATABASE', '1') ORDER BY n" 2>&1| grep -Fac "database_replicated_allow_replicated_engine_arguments"

$CLICKHOUSE_CLIENT --database_replicated_allow_replicated_engine_arguments=1 -q "CREATE TABLE $db.rmt1 (n int)
ENGINE=ReplicatedMergeTree('/test/$CLICKHOUSE_DATABASE', '1') ORDER BY n"

$CLICKHOUSE_CLIENT --database_replicated_allow_replicated_engine_arguments=2 -q "CREATE TABLE $db.rmt2 (n int)
ENGINE=ReplicatedMergeTree('/test/$CLICKHOUSE_DATABASE', '1') ORDER BY n"


$CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database='$db' ORDER BY name"

$CLICKHOUSE_CLIENT -q "SELECT substring(toString(uuid) as s, 1, length(s) - 2) FROM system.tables WHERE database='$db' and name='m1'"
$CLICKHOUSE_CLIENT -q "SELECT toString(uuid) LIKE '02858000%' FROM system.tables WHERE database='$db' and name='m2'"

$CLICKHOUSE_CLIENT -q "SHOW CREATE $db.rmt1"
$CLICKHOUSE_CLIENT -q "SHOW CREATE $db.rmt2"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS rdb_$CLICKHOUSE_DATABASE"
