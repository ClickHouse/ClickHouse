#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: connects over the MySQL and PostgreSQL wire protocols (needs psql), not enabled in fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `database` is a real setting, so a profile that constrains it must reject a database chosen
# out-of-band in a protocol handshake (native TCP connect, MySQL handshake, PostgreSQL startup)
# consistently with `USE`, `SET database = ...` and the HTTP `?database=...` parameter.

USER="user_04815_${CLICKHOUSE_DATABASE}"
PASSWORD="pass_04815"
CLIENT="${CLICKHOUSE_CLIENT_BINARY} --port ${CLICKHOUSE_PORT_TCP} --user ${USER} --password ${PASSWORD}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${USER} IDENTIFIED WITH plaintext_password BY '${PASSWORD}' SETTINGS database = '${CLICKHOUSE_DATABASE}' READONLY"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, SHOW ON *.* TO ${USER}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04815 (x UInt8) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04815 VALUES (1)"

echo "-- native TCP, connect-time database equal to the constrained value: allowed"
${CLIENT} --database "${CLICKHOUSE_DATABASE}" --query "SELECT currentDatabase() = '${CLICKHOUSE_DATABASE}'"

echo "-- native TCP, connect-time database different from the constrained value: rejected"
${CLIENT} --database system --query "SELECT 1" 2>&1 | grep -o -m1 "SETTING_CONSTRAINT_VIOLATION"

echo "-- USE: rejected"
${CLIENT} --query "USE system" 2>&1 | grep -o -m1 "SETTING_CONSTRAINT_VIOLATION"

echo "-- SET database: rejected"
${CLIENT} --query "SET database = 'system'" 2>&1 | grep -o -m1 "SETTING_CONSTRAINT_VIOLATION"

echo "-- HTTP ?database=: rejected"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?user=${USER}&password=${PASSWORD}&database=system" -d "SELECT 1" | grep -o -m1 "SETTING_CONSTRAINT_VIOLATION"

echo "-- MySQL handshake database equal to the constrained value: allowed"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM mysql('127.0.0.1:${CLICKHOUSE_PORT_MYSQL}', '${CLICKHOUSE_DATABASE}', 't_04815', '${USER}', '${PASSWORD}')"

echo "-- MySQL handshake database different from the constrained value: rejected"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM mysql('127.0.0.1:${CLICKHOUSE_PORT_MYSQL}', 'system', 'one', '${USER}', '${PASSWORD}')" 2>&1 | grep -o -m1 "should not be changed"

echo "-- PostgreSQL startup database equal to the constrained value: allowed"
psql "postgresql://${USER}:${PASSWORD}@localhost:${CLICKHOUSE_PORT_POSTGRESQL}/${CLICKHOUSE_DATABASE}" -t -A -c "SELECT x FROM t_04815"

echo "-- PostgreSQL startup database different from the constrained value: rejected"
psql "postgresql://${USER}:${PASSWORD}@localhost:${CLICKHOUSE_PORT_POSTGRESQL}/system" -t -A -c "SELECT 1" 2>&1 | grep -o -m1 "should not be changed"

${CLICKHOUSE_CLIENT} --query "DROP USER ${USER}"
