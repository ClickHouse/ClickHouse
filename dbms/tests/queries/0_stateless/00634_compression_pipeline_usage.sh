#!/bin/bash
set -e

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.pipe_log"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.pipe_tree"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.pipe_tiny"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.pipe_float"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.pipe_log (d UInt8 CODEC(Delta, LZ4)) ENGINE = Log"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.pipe_log VALUES (1), (2), (4), (7), (4), (16), (22)"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.pipe_tree (d Int32 CODEC(Delta, ZSTD(4))) ENGINE = MergeTree order by d"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.pipe_tree VALUES (1), (12), (3), (3), (4), (3), (5)"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.pipe_tiny (d UInt16 CODEC(lz4, zstd)) ENGINE = TinyLog"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.pipe_tiny VALUES (1), (2), (3), (3), (6), (4), (5)"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.pipe_float (d Float32 CODEC(Delta)) ENGINE = Log"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.pipe_float VALUES (1.5), (2.5), (3.5), (6.5), (3.5), (4.5), (5.5)"

echo
${CLICKHOUSE_CLIENT} --query "SELECT d FROM test.pipe_log"
echo
${CLICKHOUSE_CLIENT} --query "SELECT d FROM test.pipe_tree"
echo
${CLICKHOUSE_CLIENT} --query "SELECT d FROM test.pipe_tiny"
echo
${CLICKHOUSE_CLIENT} --query "SELECT d FROM test.pipe_float"


${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.pipe_log"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.pipe_tree"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.pipe_tiny"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.pipe_float"
