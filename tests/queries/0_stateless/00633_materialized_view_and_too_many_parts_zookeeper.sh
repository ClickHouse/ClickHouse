#!/usr/bin/env bash
# Tags: zookeeper

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS root"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS a"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS b"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS c"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE root (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/root', '1') ORDER BY d"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW a (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/a', '1') ORDER BY d AS SELECT * FROM root"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW b (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/b', '1') ORDER BY d SETTINGS parts_to_delay_insert=1, parts_to_throw_insert=1 AS SELECT * FROM root"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW c (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/c', '1') ORDER BY d AS SELECT * FROM root"

${CLICKHOUSE_CLIENT} --query "INSERT INTO root VALUES (1)";
${CLICKHOUSE_CLIENT} --query "SELECT _table, d FROM merge('${CLICKHOUSE_DATABASE}', '^[abc]\$') ORDER BY _table"

query_prefix="$CLICKHOUSE_DATABASE"
query_id="${query_prefix}_insert"
${CLICKHOUSE_CLIENT} --query-id="${query_id}" --materialized_views_ignore_errors=1 --query "INSERT INTO root VALUES (2)" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_views_log";
${CLICKHOUSE_CLIENT} --query "SELECT view_name, status FROM system.query_views_log WHERE initial_query_id = '${query_id}' ORDER BY view_name ASC" | sed 's/ExceptionWhileProcessing/Ex*WhileProcessing/g'

echo
${CLICKHOUSE_CLIENT} --query "SELECT _table, d FROM merge('${CLICKHOUSE_DATABASE}', '^[abc]\$') ORDER BY _table, d"

${CLICKHOUSE_CLIENT} --query "DROP TABLE root"
${CLICKHOUSE_CLIENT} --query "DROP TABLE a"
${CLICKHOUSE_CLIENT} --query "DROP TABLE b"
${CLICKHOUSE_CLIENT} --query "DROP TABLE c"

# Deduplication check for non-replicated root table
echo
${CLICKHOUSE_CLIENT} --query "CREATE TABLE root (d UInt64) ENGINE = Null"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW d (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/d', '1') ORDER BY d AS SELECT * FROM root"
${CLICKHOUSE_CLIENT} --query "INSERT INTO root SETTINGS deduplicate_blocks_in_dependent_materialized_views=1 VALUES (1)";
${CLICKHOUSE_CLIENT} --query "INSERT INTO root SETTINGS deduplicate_blocks_in_dependent_materialized_views=1 VALUES (1)";
${CLICKHOUSE_CLIENT} --query "SELECT * FROM d";
${CLICKHOUSE_CLIENT} --query "DROP TABLE root"
${CLICKHOUSE_CLIENT} --query "DROP TABLE d"
