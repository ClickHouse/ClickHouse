#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: the test creates its own Atomic database with `lazy_load_tables = 1`
#   and an explicit ReplicatedMergeTree ZooKeeper path, which would conflict with the DDL
#   replication mechanism of DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: a table in a database created with `lazy_load_tables = 1` stays wrapped in a
# `StorageTableProxy` until first access. Before this fix, `SYSTEM SYNC REPLICA` on such a table
# failed with `BAD_ARGUMENTS: Table ... is not replicated`, because the interpreter cast the proxy
# directly to `StorageReplicatedMergeTree` instead of materializing it first.

LAZY_DB="${CLICKHOUSE_DATABASE}_lazy"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${LAZY_DB}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${LAZY_DB} ENGINE = Atomic SETTINGS lazy_load_tables = 1"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${LAZY_DB}.t (a UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/lazy_sync_replica', 'r1')
    ORDER BY a
"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${LAZY_DB}.t VALUES (1)"

${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${LAZY_DB}"
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE ${LAZY_DB}"

# Confirm the table is still an unmaterialized proxy at this point.
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${LAZY_DB}' AND name = 't'"

# This must succeed without first touching the table, i.e. without materializing the proxy
# through any other path.
${CLICKHOUSE_CLIENT} -q "SYSTEM SYNC REPLICA ${LAZY_DB}.t"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${LAZY_DB}.t"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${LAZY_DB}"
