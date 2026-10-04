#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: `ON CLUSTER` is not allowed for a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (`db.ns.t` for the table `ns.t` of the database `db`; see 05077_hierarchical_names) in
# `ON CLUSTER` queries: the name is resolved on every host of the cluster, and the database may exist on the
# other hosts only.

db=$CLICKHOUSE_DATABASE

function run()
{
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 | sed "s/${db}/db/g"
}

run "CREATE TABLE ${db}.\"ns.t\" (x UInt8) ENGINE = Memory"

run "CREATE TABLE ${db}_nonexistent.t ON CLUSTER test_shard_localhost (x UInt8) ENGINE = Memory" | grep -o 'UNKNOWN_DATABASE' | sort -u
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "CREATE TABLE ${db}.ns.oc ON CLUSTER test_shard_localhost (x UInt8) ENGINE = Memory"
run "EXISTS TABLE ${db}.\"ns.oc\""
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "RENAME TABLE ${db}.ns.oc TO ${db}.ns.oc2 ON CLUSTER test_shard_localhost"
run "EXISTS TABLE ${db}.\"ns.oc2\""
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "DROP TABLE ${db}.ns.oc2 ON CLUSTER test_shard_localhost"
run "SHOW TABLES FROM ${db}"
