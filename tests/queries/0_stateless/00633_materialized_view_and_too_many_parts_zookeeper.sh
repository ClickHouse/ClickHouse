#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS root"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS a"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS b"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS c"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE root (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test_00633/root', '1') ORDER BY d"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW a (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test_00633/a', '1') ORDER BY d AS SELECT * FROM root"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW b (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test_00633/b', '1') ORDER BY d SETTINGS parts_to_delay_insert=1, parts_to_throw_insert=1 AS SELECT * FROM root"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW c (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test_00633/c', '1') ORDER BY d AS SELECT * FROM root"

${CLICKHOUSE_CLIENT} --query "INSERT INTO root VALUES (1)";
${CLICKHOUSE_CLIENT} --query "SELECT _table, d FROM merge('${CLICKHOUSE_DATABASE}', '^[abc]\$') ORDER BY _table"
if ${CLICKHOUSE_CLIENT} --query "INSERT INTO root VALUES (2)" 2>/dev/null; then
    echo "FAIL"
    echo "Expected 'too many parts' on table b"
fi

echo
${CLICKHOUSE_CLIENT} --query "SELECT _table, d FROM merge('${CLICKHOUSE_DATABASE}', '^[abc]\$') ORDER BY _table"

${CLICKHOUSE_CLIENT} --query "DROP TABLE root"
${CLICKHOUSE_CLIENT} --query "DROP TABLE a"
${CLICKHOUSE_CLIENT} --query "DROP TABLE b"
${CLICKHOUSE_CLIENT} --query "DROP TABLE c"

# Deduplication check for non-replicated root table
echo
${CLICKHOUSE_CLIENT} --query "CREATE TABLE root (d UInt64) ENGINE = Null"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW d (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test_00633/d', '1') ORDER BY d AS SELECT * FROM root"
${CLICKHOUSE_CLIENT} --query "INSERT INTO root VALUES (1)";
${CLICKHOUSE_CLIENT} --query "INSERT INTO root VALUES (1)";
${CLICKHOUSE_CLIENT} --query "SELECT * FROM d";
${CLICKHOUSE_CLIENT} --query "DROP TABLE root"
${CLICKHOUSE_CLIENT} --query "DROP TABLE d"
