#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel

# Regression test: TablesStatusRequest sent before interserver authentication
# should cause the connection to be closed without leaking table information.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE IF NOT EXISTS t_interserver_auth_check (x UInt64) ENGINE = MergeTree ORDER BY x"

python3 "$CURDIR"/04036_interserver_tables_status_auth.python

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_interserver_auth_check"
