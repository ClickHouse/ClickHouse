#!/usr/bin/env bash
# Tags: no-replicated-database
# ^ creates a database.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db_extra="${CLICKHOUSE_DATABASE}_extra"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${db_extra}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${db_extra}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db_extra}.t (x UInt8) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS merge"
$CLICKHOUSE_CLIENT -q "CREATE TABLE merge ENGINE = Merge(${db_extra}, 't')"
$CLICKHOUSE_CLIENT -q "SELECT * FROM merge"
$CLICKHOUSE_CLIENT -q "SELECT table, total_rows, total_bytes FROM system.tables WHERE database = currentDatabase() AND table = 'merge'"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${db_extra}"
$CLICKHOUSE_CLIENT -q "SELECT * FROM merge" 2>&1 | grep -o 'UNKNOWN_DATABASE' | head -1
# Even when the database behind the merge table does not exist anymore, querying the 'total_rows' field from system.tables does not throw an exception.
# Suppress stderr because StorageMerge::totalRows/totalBytes logs server-side errors when the underlying database is gone.
$CLICKHOUSE_CLIENT -q "SELECT table, total_rows, total_bytes FROM system.tables WHERE database = currentDatabase() AND table = 'merge'" 2>/dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE merge"
