#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database

# Issue #106087: ON CLUSTER pass-through for Replicated databases.
# When the cluster name in ON CLUSTER matches the automatic cluster name of a
# Replicated database (the database name itself), the clause is treated as a
# no-op for RENAME / EXCHANGE, since the database engine already propagates DDL.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A unique database name per test run keeps the test parallel-safe; the cluster
# name in ON CLUSTER must equal this database name (its automatic cluster).
db="rdb_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${db}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${db} ENGINE = Replicated('/clickhouse/test/{database}/04302', 's1', 'r1')"

# CREATE / ALTER with ON CLUSTER == database name (already worked before the fix).
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.t1 ON CLUSTER ${db} (a Int, b Int) ENGINE = ReplicatedMergeTree ORDER BY a" >/dev/null
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${db}.t1 ON CLUSTER ${db} ADD COLUMN c Int" >/dev/null
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.columns WHERE database = '${db}' AND table = 't1' ORDER BY name"

$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.t1 (a, b, c) VALUES (1, 1, 1)"

# RENAME TABLE with ON CLUSTER == database name (added by this fix).
$CLICKHOUSE_CLIENT -q "RENAME TABLE ${db}.t1 TO ${db}.t1_renamed ON CLUSTER ${db}" >/dev/null
$CLICKHOUSE_CLIENT -q "SELECT 'after rename', count() FROM ${db}.t1_renamed"

# EXCHANGE TABLES with ON CLUSTER == database name (added by this fix).
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.s (a Int, b Int, c Int) ENGINE = ReplicatedMergeTree ORDER BY a" >/dev/null
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.s (a, b, c) VALUES (2, 2, 2)"
$CLICKHOUSE_CLIENT -q "EXCHANGE TABLES ${db}.t1_renamed AND ${db}.s ON CLUSTER ${db}" >/dev/null
$CLICKHOUSE_CLIENT -q "SELECT 't1_renamed after exchange', * FROM ${db}.t1_renamed ORDER BY a, b, c"
$CLICKHOUSE_CLIENT -q "SELECT 's after exchange', * FROM ${db}.s ORDER BY a, b, c"

# ON CLUSTER with a non-matching cluster name still fails (cluster lookup runs
# through executeDDLQueryOnCluster).
$CLICKHOUSE_CLIENT -q "RENAME TABLE ${db}.t1_renamed TO ${db}.t2 ON CLUSTER 'does_not_exist_cluster'" 2>&1 | grep -o "CLUSTER_DOESNT_EXIST" | head -n 1

# The existing ignore_on_cluster_for_replicated_database setting keeps working
# for RENAME (no regression).
$CLICKHOUSE_CLIENT -q "RENAME TABLE ${db}.t1_renamed TO ${db}.t1_back ON CLUSTER 'does_not_exist_cluster' SETTINGS ignore_on_cluster_for_replicated_database = 1" >/dev/null
$CLICKHOUSE_CLIENT -q "SELECT 'final', count() FROM ${db}.t1_back"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${db} SYNC"
