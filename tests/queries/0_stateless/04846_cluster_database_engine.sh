#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `Cluster` database engine provides real-time access to the tables of a database on a cluster
# from the server configuration. The test clusters used here (`test_shard_localhost`,
# `test_cluster_two_shards`) point back to this same server.

CLUSTER_DB="${CLICKHOUSE_DATABASE}_cluster"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLUSTER_DB}"

# A local table on the "cluster" (the current database).
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${CLICKHOUSE_DATABASE}.t VALUES (1, 'a'), (2, 'b'), (3, 'c');
"

# The cluster name is usually written as an identifier, like in the `Distributed` table engine.
${CLICKHOUSE_CLIENT} --query "
    CREATE DATABASE ${CLUSTER_DB} ENGINE = Cluster(test_shard_localhost, '${CLICKHOUSE_DATABASE}')
"

echo '-- database engine'
${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.databases WHERE name = '${CLUSTER_DB}'"

echo '-- SHOW CREATE DATABASE prints the engine with the cluster name'
${CLICKHOUSE_CLIENT} --query "SHOW CREATE DATABASE ${CLUSTER_DB} FORMAT TSVRaw" | grep -c "Cluster('test_shard_localhost'"

echo '-- SHOW TABLES lists the tables of the database on the cluster'
${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${CLUSTER_DB}"

echo '-- each table is exposed as a Distributed storage'
${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.tables WHERE database = '${CLUSTER_DB}' AND name = 't'"

echo '-- DESCRIBE reflects the remote structure'
${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE ${CLUSTER_DB}.t" | cut -f1,2

echo '-- SELECT is forwarded to the cluster'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${CLUSTER_DB}.t ORDER BY id"

echo '-- INSERT is forwarded to the cluster'
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLUSTER_DB}.t VALUES (4, 'd')"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${CLICKHOUSE_DATABASE}.t ORDER BY id"

echo '-- EXISTS TABLE for an existing and a missing table'
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${CLUSTER_DB}.t"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${CLUSTER_DB}.does_not_exist"

echo '-- a SELECT from a missing table reports UNKNOWN_TABLE and must not recurse into the name hints'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${CLUSTER_DB}.no_such_table" 2>&1 | grep -c -m1 "UNKNOWN_TABLE"

echo '-- the cluster name may come from a macro, like in the Distributed table engine'
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLUSTER_DB}_macro ENGINE = Cluster('{default_cluster_macro}', '${CLICKHOUSE_DATABASE}')"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${CLUSTER_DB}_macro.t"
echo '-- SHOW CREATE TABLE cannot serialize a cluster name that is expanded on every access'
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${CLUSTER_DB}_macro.t" 2>&1 | grep -c -m1 "THERE_IS_NO_QUERY"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLUSTER_DB}_macro"

echo '-- a multi-shard database reads from every shard and accepts INSERT queries by default'
# Both shards of `test_cluster_two_shards` point to this server, so a SELECT doubles the rows, and
# an INSERT (distributed by the implicit rand() sharding key) lands in the same local table,
# adding exactly one row wherever it goes.
${CLICKHOUSE_CLIENT} --query "
    CREATE DATABASE ${CLUSTER_DB}_sharded ENGINE = Cluster('test_cluster_two_shards', '${CLICKHOUSE_DATABASE}');
    SELECT count() FROM ${CLUSTER_DB}_sharded.t;
    INSERT INTO ${CLUSTER_DB}_sharded.t VALUES (5, 'e');
    SELECT count() FROM ${CLICKHOUSE_DATABASE}.t;
"

echo '-- SHOW CREATE TABLE cannot serialize a multi-shard proxy with an insert-only sharding key'
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${CLUSTER_DB}_sharded.t" 2>&1 | grep -c -m1 "THERE_IS_NO_QUERY"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLUSTER_DB}_sharded"

echo '-- SHOW CREATE TABLE cannot serialize a reloadable single-shard proxy either'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.m (a UInt32, b UInt32 DEFAULT a + 1, c UInt32 ALIAS a + 2, d UInt32 MATERIALIZED a + 3) ENGINE = MergeTree ORDER BY a;
"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${CLUSTER_DB}.m" 2>&1 | grep -c -m1 "THERE_IS_NO_QUERY"

echo '-- an unknown cluster is rejected at CREATE (prints 1 if the expected error is raised)'
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLUSTER_DB}_unknown ENGINE = Cluster('there_is_no_such_cluster', 'default')" 2>&1 | grep -c -m1 "CLUSTER_DOESNT_EXIST"

echo '-- a wrong number of arguments is rejected (prints 1 if the expected error is raised)'
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLUSTER_DB}_bad ENGINE = Cluster('test_shard_localhost')" 2>&1 | grep -c -m1 "BAD_ARGUMENTS"

echo '-- a database that refers to itself is rejected at CREATE instead of recursing (prints 1 if the expected error is raised)'
# A lazily-reported cycle would fail every whole-server scan (e.g. `system.tables`) for every user
# while the database exists, so the chain is rejected when it is being created.
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLUSTER_DB}_loop ENGINE = Cluster('test_shard_localhost', '${CLUSTER_DB}_loop')" 2>&1 | grep -c -m1 "INFINITE_LOOP"

echo '-- a CREATE that would complete a cycle through a Remote database is rejected (prints 1 if the expected error is raised)'
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLUSTER_DB}_cycle_a ENGINE = Cluster('test_shard_localhost', '${CLUSTER_DB}_cycle_b')"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLUSTER_DB}_cycle_b ENGINE = Remote('127.0.0.1', '${CLUSTER_DB}_cycle_a', 'default', '')" 2>&1 | grep -c -m1 "INFINITE_LOOP"
# The half-open chain (its target database does not exist) must not affect whole-server scans, and
# it lists no tables.
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.tables"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${CLUSTER_DB}_cycle_a'"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLUSTER_DB}_cycle_a"

echo '-- an explicit ATTACH DATABASE is a user query and is validated exactly like CREATE'
# Only the internal metadata replay of server startup skips the checks, so neither the
# cluster-name validation nor the cycle rejection can be bypassed by attaching.
${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE ${CLUSTER_DB}_attach ENGINE = Cluster('there_is_no_such_cluster', 'default')" 2>&1 | grep -c -m1 "CLUSTER_DOESNT_EXIST"
${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE ${CLUSTER_DB}_attach ENGINE = Cluster(test_shard_localhost, '${CLUSTER_DB}_attach')" 2>&1 | grep -c -m1 "INFINITE_LOOP"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${CLUSTER_DB}_attach'"

echo '-- a shard that is unavailable fails the SELECT with the real error, while the metadata is served by the available shard'
# The first shard of `test_unavailable_shard` is this server, the second one points to a port that
# is never listened on. The metadata comes from an arbitrary available shard (the local one), so the
# listing works; reading goes to every shard and reports the connection error.
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLUSTER_DB}_unavailable ENGINE = Cluster('test_unavailable_shard', '${CLICKHOUSE_DATABASE}')"
${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${CLUSTER_DB}_unavailable LIKE 't'"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${CLUSTER_DB}_unavailable.t" 2>&1 | grep -c -m1 -E "NETWORK_ERROR|ALL_CONNECTION_TRIES_FAILED|CONNECTION_REFUSED"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLUSTER_DB}_unavailable"

echo '-- DDL against a Cluster database is not supported (prints 1 if the expected error is raised)'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${CLUSTER_DB}.new_table (x UInt8) ENGINE = Memory" 2>&1 | grep -c -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLUSTER_DB}.t" 2>&1 | grep -c -m1 "NOT_IMPLEMENTED"
# TRUNCATE reaches the proxy storage directly, where it would be a silent no-op of
# `StorageDistributed`; it must be rejected as well, and the data must stay intact.
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE ${CLUSTER_DB}.t" 2>&1 | grep -c -m1 "read-through database proxy"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLUSTER_DB}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.t"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.m"
