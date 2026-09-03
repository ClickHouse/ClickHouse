#!/usr/bin/env bash

# Test that system.tables `SELECT name` / `SELECT database, name` fast path (fillTableNamesOnly)
# respects SHOW_TABLES access correctly after removing the redundant per-table isGranted check.
#
# Three scenarios:
#   A. Global SHOW_TABLES grant → fast path is taken, all tables visible
#   B. Database-level SHOW_TABLES grant → fast path is taken, all tables in db visible
#   C. Per-table SHOW_TABLES grant → slow path is taken, only granted table visible

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${DB}.t1 (x UInt32) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${DB}.t2 (x UInt32) ENGINE = Memory;"

# --- Case A: global SHOW_TABLES grant ---
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${DB}_global_user;"
$CLICKHOUSE_CLIENT --query "CREATE USER ${DB}_global_user IDENTIFIED WITH no_password;"
$CLICKHOUSE_CLIENT --query "GRANT SHOW TABLES ON *.* TO ${DB}_global_user;"

echo "=== Global grant: SELECT name (fast path) shows both tables ==="
$CLICKHOUSE_CLIENT --user="${DB}_global_user" \
    --query "SELECT name FROM system.tables WHERE database = '${DB}' ORDER BY name;"

echo "=== Global grant: SELECT database, name (fast path) shows both tables ==="
$CLICKHOUSE_CLIENT --user="${DB}_global_user" \
    --query "SELECT database, name FROM system.tables WHERE database = '${DB}' ORDER BY name;" \
    | sed "s/^${DB}\t/{db}\t/"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${DB}_global_user;"

# --- Case B: database-level SHOW_TABLES grant ---
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${DB}_db_user;"
$CLICKHOUSE_CLIENT --query "CREATE USER ${DB}_db_user IDENTIFIED WITH no_password;"
$CLICKHOUSE_CLIENT --query "GRANT SHOW TABLES ON ${DB}.* TO ${DB}_db_user;"

echo "=== DB-level grant: SELECT name (fast path) shows both tables ==="
$CLICKHOUSE_CLIENT --user="${DB}_db_user" \
    --query "SELECT name FROM system.tables WHERE database = '${DB}' ORDER BY name;"

echo "=== DB-level grant: SELECT database, name (fast path) shows both tables ==="
$CLICKHOUSE_CLIENT --user="${DB}_db_user" \
    --query "SELECT database, name FROM system.tables WHERE database = '${DB}' ORDER BY name;" \
    | sed "s/^${DB}\t/{db}\t/"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${DB}_db_user;"

# --- Case C: per-table SHOW_TABLES grant (slow path — main loop with per-table check) ---
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${DB}_table_user;"
$CLICKHOUSE_CLIENT --query "CREATE USER ${DB}_table_user IDENTIFIED WITH no_password;"
$CLICKHOUSE_CLIENT --query "GRANT SHOW TABLES ON ${DB}.t1 TO ${DB}_table_user;"

echo "=== Per-table grant: SELECT name (slow path) shows only t1 ==="
$CLICKHOUSE_CLIENT --user="${DB}_table_user" \
    --query "SELECT name FROM system.tables WHERE database = '${DB}' ORDER BY name;"

echo "=== Per-table grant: SELECT database, name (slow path) shows only t1 ==="
$CLICKHOUSE_CLIENT --user="${DB}_table_user" \
    --query "SELECT database, name FROM system.tables WHERE database = '${DB}' ORDER BY name;" \
    | sed "s/^${DB}\t/{db}\t/"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${DB}_table_user;"
