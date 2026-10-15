#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: `SYSTEM DROP REPLICA` reaches server-wide state.
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
# no-shared-merge-tree: the table engine is replaced.

# `SYSTEM DROP REPLICA ... FROM ZKPATH` refuses to drop the replica of a live local table. With `lazy_load_tables`
# a table that nothing has accessed yet is a stand-in holding no session in ZooKeeper, so the guard has to resolve
# its path from the stored `CREATE TABLE` query, without loading the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"
UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS $DB;
    CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE $DB.t_explicit (a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05333_t', 'r1') ORDER BY a;
    CREATE TABLE $DB.t_macros UUID '$UUID' (a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY a;
"

# The paths as the loaded tables see them.
read -r explicit_path explicit_replica < <($CLICKHOUSE_CLIENT -q "SELECT zookeeper_path, replica_name FROM system.replicas WHERE database = '$DB' AND table = 't_explicit'")
read -r macros_path macros_replica < <($CLICKHOUSE_CLIENT -q "SELECT zookeeper_path, replica_name FROM system.replicas WHERE database = '$DB' AND table = 't_macros'")

# A stand-in appears when the database is loaded.
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB; ATTACH DATABASE $DB;"
$CLICKHOUSE_CLIENT -q "SELECT 'stand-ins', name, engine FROM system.tables WHERE database = '$DB' ORDER BY name"

$CLICKHOUSE_CLIENT -q "SYSTEM DROP REPLICA '$explicit_replica' FROM ZKPATH '$explicit_path'" 2>&1 | grep -o -m1 "There is a local table ${DB}.t_explicit"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP REPLICA '$macros_replica' FROM ZKPATH '$macros_path'" 2>&1 | grep -o -m1 "There is a local table ${DB}.t_macros"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/clickhouse/tables/${CLICKHOUSE_DATABASE}_05333_nonexistent'" 2>&1 | grep -o -m1 "does not look like a table path"

# The guard did not load the tables, and their replicas are still there.
$CLICKHOUSE_CLIENT -q "SELECT 'still stand-ins', name, engine FROM system.tables WHERE database = '$DB' ORDER BY name"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA $DB.t_explicit; SYSTEM SYNC REPLICA $DB.t_macros;"
$CLICKHOUSE_CLIENT -q "SELECT 'loaded', table, is_readonly FROM system.replicas WHERE database = '$DB' ORDER BY table"

$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB"
