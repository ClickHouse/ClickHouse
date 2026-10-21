#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: depends on libpqxx (the PostgreSQL table engine), which is not built in fast test.
#
# A `PostgreSQL` table takes the connection pool settings its definition does not state from the session that creates
# it. A new table adds those that differ from the defaults of the engine to its `SETTINGS`, so that it keeps them when it
# is attached again in another context, e.g. on server startup. The engine does not connect at `CREATE` or `ATTACH` when the
# columns are given, so the host does not need to be reachable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ENGINE="PostgreSQL('127.0.0.1:5432', 'db', 'tbl', 'user', 'password')"
# A `Replicated` database prints the status of each DDL query otherwise.
DDL_CLIENT="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none"

function settings_of()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = '$1'"
}

echo "--- values taken from the session are stored"
${DDL_CLIENT} --postgresql_connection_pool_size 3 --postgresql_connection_pool_auto_close_connection 1 \
    --query "CREATE TABLE t_session (x Int32) ENGINE = ${ENGINE}"
settings_of t_session

echo "--- values equal to the defaults of the engine are not stored"
${DDL_CLIENT} --query "CREATE TABLE t_server (x Int32) ENGINE = ${ENGINE}"
settings_of t_server

echo "--- a stated value is kept as stated"
${DDL_CLIENT} --postgresql_connection_pool_size 3 \
    --query "CREATE TABLE t_stated (x Int32) ENGINE = ${ENGINE} SETTINGS postgresql_connection_pool_size = 5"
settings_of t_stated

echo "--- a value reset with DEFAULT keeps the default of the engine"
${DDL_CLIENT} --send_logs_level trace --postgresql_connection_pool_size 3 \
    --query "CREATE TABLE t_default (x Int32) ENGINE = ${ENGINE} SETTINGS postgresql_connection_pool_size = DEFAULT" 2>&1 \
    | grep -o -m1 'PostgreSQL connection pool size: [0-9]*'
settings_of t_default

echo "--- an ATTACH with the full definition stores them too"
# An `Atomic` or `Replicated` database attaches a table with a full definition only with its UUID; `Ordinary` has none.
UUID_CLAUSE=""
if [ "$(${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.databases WHERE name = currentDatabase()")" != "Ordinary" ]; then
    UUID_CLAUSE="UUID '$(${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()")'"
fi
# The server warns that an ATTACH with the full definition is not recommended.
${DDL_CLIENT} --send_logs_level fatal --postgresql_connection_pool_size 3 --query "ATTACH TABLE t_attach ${UUID_CLAUSE} (x Int32) ENGINE = ${ENGINE}"
settings_of t_attach

echo "--- the table keeps the stored value when attached in a session without it"
${DDL_CLIENT} --query "DETACH TABLE t_session PERMANENTLY"
${DDL_CLIENT} --send_logs_level trace --query "ATTACH TABLE t_session" 2>&1 \
    | grep -o -m1 'PostgreSQL connection pool size: [0-9]*'

echo "--- a RESTORE keeps the definition from the backup"
# `RESTORE` rejects a restored table whose definition differs from the one in the backup, so nothing is added to it.
${DDL_CLIENT} --query "CREATE TABLE t_backup (x Int32) ENGINE = ${ENGINE}"
${DDL_CLIENT} --query "
    BACKUP TABLE t_backup TO Memory('${CLICKHOUSE_TEST_UNIQUE_NAME}') FORMAT Null;
    SET postgresql_connection_pool_size = 3;
    RESTORE TABLE t_backup AS t_restored FROM Memory('${CLICKHOUSE_TEST_UNIQUE_NAME}') FORMAT Null;"
settings_of t_restored

echo "--- a RESTORE of a materialized view keeps the definitions from the backup"
# The view creates its inner table with the view's `ENGINE` clause, which the two definitions share.
${DDL_CLIENT} --query "CREATE MATERIALIZED VIEW mv_backup (x Int32) ENGINE = ${ENGINE} AS SELECT 1 AS x"
${DDL_CLIENT} --query "
    BACKUP TABLE mv_backup TO Memory('${CLICKHOUSE_TEST_UNIQUE_NAME}_mv') FORMAT Null;
    SET postgresql_connection_pool_size = 3;
    RESTORE TABLE mv_backup AS mv_restored FROM Memory('${CLICKHOUSE_TEST_UNIQUE_NAME}_mv') FORMAT Null;"
settings_of mv_restored
