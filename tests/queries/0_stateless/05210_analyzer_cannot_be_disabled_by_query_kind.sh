#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The analyzer is mandatory since 26.9. A setting given to `clickhouse-local` on the command line is
# not checked against the settings constraints, so a value that would disable it is ignored instead.
echo "-- clickhouse-local, both names"
$CLICKHOUSE_LOCAL --allow_experimental_analyzer 0 --query "SELECT toUInt8(getSetting('allow_experimental_analyzer'))"
$CLICKHOUSE_LOCAL --enable_analyzer 0 --query "SELECT toUInt8(getSetting('enable_analyzer'))"

# The one query that keeps the value it was given is a query another server sent, so that the servers
# of a cluster agree on how one query is analyzed. Such a query is identified by its kind, which a
# client can declare for itself - but `clickhouse-local` is not a server another one sends queries to,
# so the declaration does not keep the old query analysis alive there.
echo "-- clickhouse-local declaring a secondary query"
$CLICKHOUSE_LOCAL --query_kind secondary_query --allow_experimental_analyzer 0 --query "SELECT toUInt8(getSetting('allow_experimental_analyzer'))"
$CLICKHOUSE_LOCAL --query_kind secondary_query --enable_analyzer 0 --query "SELECT toUInt8(getSetting('enable_analyzer'))"

# A change sent to the server is refused rather than ignored, a command-line one included.
echo "-- clickhouse-client"
$CLICKHOUSE_CLIENT --allow_experimental_analyzer 0 --query "SELECT 1" 2>&1 | grep -o -m1 'SETTING_CONSTRAINT_VIOLATION'
