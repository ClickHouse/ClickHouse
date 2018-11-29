#!/usr/bin/env bash
set -e

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.root"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.a"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.b"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.c"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.root (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test/root', '1') ORDER BY d"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW test.a (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test/a', '1') ORDER BY d AS SELECT * FROM test.root"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW test.b (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test/b', '1') ORDER BY d SETTINGS parts_to_delay_insert=1, parts_to_throw_insert=1 AS SELECT * FROM test.root"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW test.c (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test/c', '1') ORDER BY d AS SELECT * FROM test.root"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test.root VALUES (1)";
${CLICKHOUSE_CLIENT} --query "SELECT _table, d FROM merge('test', '^[abc]\$') ORDER BY _table"
if ${CLICKHOUSE_CLIENT} --query "INSERT INTO test.root VALUES (2)" 2>/dev/null; then
	echo "FAIL\nExpected 'too many parts' on table test.b"
fi

echo
${CLICKHOUSE_CLIENT} --query "SELECT _table, d FROM merge('test', '^[abc]\$') ORDER BY _table"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.root"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.a"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.b"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.c"

# Deduplication check for non-replicated root table
echo
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.root (d UInt64) ENGINE = Null"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW test.a (d UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/test/a', '1') ORDER BY d AS SELECT * FROM test.root"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.root VALUES (1)";
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.root VALUES (1)";
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test.a";
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.root"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.a"
