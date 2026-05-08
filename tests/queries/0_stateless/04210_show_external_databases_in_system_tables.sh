#!/usr/bin/env bash
# Tags: no-fasttest

# Verify that PostgreSQL databases (and other external databases) are hidden from system.tables / system.columns
# by default, and shown when `show_external_databases_in_system_tables = 1`.
# Also verifies that the old name `show_data_lake_catalogs_in_system_tables` works as an alias.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NEW_DB="${CLICKHOUSE_DATABASE}_pg_external"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${NEW_DB};"

# PostgreSQL database engine does not connect at CREATE time, so a non-existent host is fine here.
$CLICKHOUSE_CLIENT -q "
CREATE DATABASE ${NEW_DB} ENGINE = PostgreSQL('192.0.2.1:5432', 'fake_db', 'user', 'password');
"

# system.databases always shows the database, regardless of the setting.
$CLICKHOUSE_CLIENT -q "SELECT engine FROM system.databases WHERE name = '${NEW_DB}' SETTINGS show_external_databases_in_system_tables = 0;"
$CLICKHOUSE_CLIENT -q "SELECT engine FROM system.databases WHERE name = '${NEW_DB}' SETTINGS show_external_databases_in_system_tables = 1;"

# system.tables hides the database by default; counting must be 0 without contacting the server.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '${NEW_DB}' SETTINGS show_external_databases_in_system_tables = 0;"

# system.columns hides the database by default.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns WHERE database = '${NEW_DB}' SETTINGS show_external_databases_in_system_tables = 0;"

# The old setting name still works as an alias.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '${NEW_DB}' SETTINGS show_data_lake_catalogs_in_system_tables = 0;"

# system.settings exposes both the new name and the alias.
$CLICKHOUSE_CLIENT -q "SELECT name, alias_for FROM system.settings WHERE name IN ('show_external_databases_in_system_tables', 'show_data_lake_catalogs_in_system_tables') ORDER BY name;"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${NEW_DB};"
