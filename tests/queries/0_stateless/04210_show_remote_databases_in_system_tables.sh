#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag justification:
#   no-fasttest: depends on libpq and libmysql (PostgreSQL and MySQL database engines),
#     which are not built in fast test.
#   no-parallel: creates PostgreSQL and MySQL databases pointing at an unreachable host.
#     Because `show_remote_databases_in_system_tables` defaults to `true`, these databases
#     are visible in `system.tables`, `system.columns` and `system.completions`, so any
#     concurrent query that scans those tables without a database filter would try to
#     connect to the unreachable host and fail with `POSTGRESQL_CONNECTION_FAILURE`.

set -e

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PG_DB="${CLICKHOUSE_DATABASE}_pg_remote"
MYSQL_DB="${CLICKHOUSE_DATABASE}_mysql_remote"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${PG_DB}; DROP DATABASE IF EXISTS ${MYSQL_DB};"

# PostgreSQL database engine does not connect at CREATE time, so a non-existent host is fine here.
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${PG_DB} ENGINE = PostgreSQL('192.0.2.1:5432', 'fake_db', 'user', 'password');"

# MySQL probes the server at CREATE time, but is tolerant of failures on ATTACH.
# Use a short connect_timeout / single try so the tolerated failure happens fast.
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${MYSQL_DB} ENGINE = MySQL('192.0.2.1:3306', 'fake_db', 'user', 'password') SETTINGS connect_timeout = 1, connection_max_tries = 1;"

$CLICKHOUSE_CLIENT -n -q "
SELECT '--- system.databases always shows remote databases, regardless of the setting ---';
SELECT engine FROM system.databases WHERE name IN ('${PG_DB}', '${MYSQL_DB}') ORDER BY engine SETTINGS show_remote_databases_in_system_tables = 0;
SELECT engine FROM system.databases WHERE name IN ('${PG_DB}', '${MYSQL_DB}') ORDER BY engine SETTINGS show_remote_databases_in_system_tables = 1;

SELECT '--- system.tables hides PostgreSQL when disabled (count must be 0 without contacting the server) ---';
USE ${PG_DB};
SELECT count() FROM system.tables WHERE database = currentDatabase() SETTINGS show_remote_databases_in_system_tables = 0;

SELECT '--- system.tables hides MySQL when disabled ---';
USE ${MYSQL_DB};
SELECT count() FROM system.tables WHERE database = currentDatabase() SETTINGS show_remote_databases_in_system_tables = 0;

SELECT '--- system.columns hides PostgreSQL when disabled ---';
USE ${PG_DB};
SELECT count() FROM system.columns WHERE database = currentDatabase() SETTINGS show_remote_databases_in_system_tables = 0;

SELECT '--- system.columns hides MySQL when disabled ---';
USE ${MYSQL_DB};
SELECT count() FROM system.columns WHERE database = currentDatabase() SETTINGS show_remote_databases_in_system_tables = 0;

SELECT '--- system.completions does not list remote databases when disabled (count must be 0 without contacting the server) ---';
SELECT count() FROM system.completions WHERE word IN ('${PG_DB}', '${MYSQL_DB}') AND context = 'database' SETTINGS show_remote_databases_in_system_tables = 0;

SELECT '--- system.settings exposes separate data-lake and remote-database settings ---';
SELECT name, value, alias_for = '' FROM system.settings WHERE name IN ('show_remote_databases_in_system_tables', 'show_data_lake_catalogs_in_system_tables') ORDER BY name;

USE ${CLICKHOUSE_DATABASE};
DROP DATABASE ${PG_DB};
DROP DATABASE ${MYSQL_DB};
"
