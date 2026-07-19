#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `Remote` database engine provides real-time access to the tables of a database on a remote
# ClickHouse server. Here the "remote" server is this same server, reached over the network via
# 127.0.0.1 (no port is given, so the server's own tcp_port is used).

REMOTE_DB="${CLICKHOUSE_DATABASE}_remote"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${REMOTE_DB}"

# A local table on the "remote" server (the current database).
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${CLICKHOUSE_DATABASE}.t VALUES (1, 'a'), (2, 'b'), (3, 'c');
"

${CLICKHOUSE_CLIENT} --query "
    CREATE DATABASE ${REMOTE_DB} ENGINE = Remote('127.0.0.1', '${CLICKHOUSE_DATABASE}', 'default', '')
"

echo '-- database engine'
${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.databases WHERE name = '${REMOTE_DB}'"

echo '-- SHOW TABLES lists the remote tables'
${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${REMOTE_DB}"

echo '-- each table is exposed as a Distributed storage'
${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.tables WHERE database = '${REMOTE_DB}' AND name = 't'"

echo '-- DESCRIBE reflects the remote structure'
${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE ${REMOTE_DB}.t" | cut -f1,2

echo '-- SELECT is forwarded to the remote server'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${REMOTE_DB}.t ORDER BY id"

echo '-- INSERT is forwarded to the remote server'
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${REMOTE_DB}.t VALUES (4, 'd')"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${CLICKHOUSE_DATABASE}.t ORDER BY id"

echo '-- SHOW CREATE TABLE preserves column defaults, aliases and materialized expressions'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.m (a UInt32, b UInt32 DEFAULT a + 1, c UInt32 ALIAS a + 2, d UInt32 MATERIALIZED a + 3) ENGINE = MergeTree ORDER BY a;
"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${REMOTE_DB}.m FORMAT TSVRaw" | grep -oE "(DEFAULT a \+ 1|ALIAS a \+ 2|MATERIALIZED a \+ 3)" | sort

echo '-- EXISTS TABLE for an existing and a missing table'
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${REMOTE_DB}.t"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${REMOTE_DB}.does_not_exist"

echo '-- DDL against a Remote database is not supported (prints 1 if the expected error is raised)'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${REMOTE_DB}.new_table (x UInt8) ENGINE = Memory" 2>&1 | grep -c -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${REMOTE_DB}.t" 2>&1 | grep -c -m1 "NOT_IMPLEMENTED"

echo '-- the password is masked in SHOW CREATE DATABASE (0 = not leaked, 1 = [HIDDEN] shown)'
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${REMOTE_DB}_secret ENGINE = Remote('127.0.0.1', '${CLICKHOUSE_DATABASE}', 'default', 'sekret')"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE DATABASE ${REMOTE_DB}_secret" | grep -c "sekret"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE DATABASE ${REMOTE_DB}_secret" | grep -c -m1 "\[HIDDEN\]"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${REMOTE_DB}_secret"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${REMOTE_DB}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.t"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLICKHOUSE_DATABASE}.m"
