#!/usr/bin/env bash
# Tags: replica, zookeeper

# Test for CREATE TABLE IF NOT EXISTS ... AND INSERT restrictions with ReplicatedMergeTree
# in a Replicated database.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query \
    "CREATE DATABASE ${CLICKHOUSE_DATABASE}_rmt_db ENGINE = Replicated('/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}_rmt_db', '{shard}', '{replica}')" || exit 1

trap '${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_rmt_db"' EXIT

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query \
    "CREATE TABLE ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table (a UInt32, b String) ENGINE = ReplicatedMergeTree ORDER BY a" || exit 1

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query \
    "CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table (a UInt32, b String) ENGINE = MergeTree ORDER BY a AND INSERT SELECT 1 AS a, 'test' AS b; -- { serverError SUPPORT_IS_DISABLED }" || exit 1

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_heavy_create=1 --query \
    "CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table (a UInt32, b String) ENGINE = MergeTree ORDER BY a AND INSERT SELECT 2 AS a, 'test2' AS b; -- { serverError SUPPORT_IS_DISABLED }" || exit 1

${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table" || exit 1
# Cleanup
trap '${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_rmt_db"' EXIT