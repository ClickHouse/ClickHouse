#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/107538
# A bare `functions` does not exist in the current database, only in `system`.
# Both analyzers must suggest the cross-database table `system.functions`.
# `system.functions` is an exact (distance 0) match, so it is a stable hint
# even when concurrent tests have similarly named tables in other databases.
#
# grep -m1: with --send_logs_level the server also echoes the exception as a log event,
# so the hint can appear more than once; take a single match to stay deterministic.
$CLICKHOUSE_CLIENT --enable_analyzer=1 -q "SELECT * FROM functions" 2>&1 | grep -oF -m1 "Maybe you meant system.functions?"
$CLICKHOUSE_CLIENT --enable_analyzer=0 -q "SELECT * FROM functions" 2>&1 | grep -oF -m1 "Maybe you meant system.functions?"

# When the DATABASE part of a compound name does not exist, the database is resolved
# (and rejected) before the table, so the error stays UNKNOWN_DATABASE and must NOT fall
# back to a cross-database table hint such as `system.functions`. Verify both analyzers.
$CLICKHOUSE_CLIENT --enable_analyzer=1 -q "SELECT * FROM ${CLICKHOUSE_DATABASE}_missing.functions" 2>&1 | grep -oF -m1 "UNKNOWN_DATABASE"
$CLICKHOUSE_CLIENT --enable_analyzer=1 -q "SELECT * FROM ${CLICKHOUSE_DATABASE}_missing.functions" 2>&1 | grep -c -F "Maybe you meant system.functions?" || true
$CLICKHOUSE_CLIENT --enable_analyzer=0 -q "SELECT * FROM ${CLICKHOUSE_DATABASE}_missing.functions" 2>&1 | grep -oF -m1 "UNKNOWN_DATABASE"
$CLICKHOUSE_CLIENT --enable_analyzer=0 -q "SELECT * FROM ${CLICKHOUSE_DATABASE}_missing.functions" 2>&1 | grep -c -F "Maybe you meant system.functions?" || true
