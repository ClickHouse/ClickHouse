#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: the test creates its own Atomic database with `lazy_load_tables = 1`
#   and an explicit ReplicatedMergeTree ZooKeeper path, which would conflict with the DDL
#   replication mechanism of DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: a table in a database created with `lazy_load_tables = 1` stays wrapped in a
# `StorageTableProxy` until first access. Before this fix, any mutation (`ALTER ... UPDATE`,
# `MATERIALIZE TTL`, ...) on such a table failed with NOT_IMPLEMENTED "Table engine
# ReplicatedMergeTree doesn't support mutations": `StorageProxy` forwarded `mutate` but not
# `checkMutationIsPossible`, so `IStorage`'s throwing default fired with the nested engine's name.

LAZY_DB="${CLICKHOUSE_DATABASE}_lazy"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${LAZY_DB}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${LAZY_DB} ENGINE = Atomic SETTINGS lazy_load_tables = 1"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${LAZY_DB}.t (a UInt64, b UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/lazy_mutations', 'r1')
    ORDER BY a
"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${LAZY_DB}.t (a, b) VALUES (1, 10), (2, 20)"

# DETACH + ATTACH the database so the table goes back to an unmaterialized proxy: the INSERT above
# has already materialized it once, and the point is to mutate through the fresh proxy.
${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${LAZY_DB}"
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE ${LAZY_DB}"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${LAZY_DB}.t UPDATE b = b + 1 WHERE a = 1 SETTINGS mutations_sync = 2"
# NOTE: MATERIALIZE TTL through a lazy proxy is still broken differently (the proxy's cached
# in-memory metadata carries columns only, no TTL -- see the StorageProxy forwarding audit report);
# this test deliberately pins only what the checkMutationIsPossible forward fixes.

${CLICKHOUSE_CLIENT} -q "SELECT a, b FROM ${LAZY_DB}.t ORDER BY a"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${LAZY_DB}"
