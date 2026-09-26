#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The analyzer is mandatory since 26.9. A setting given to `clickhouse-local` on the command line is
# not checked against the settings constraints, so a value that would disable it is ignored instead.
echo "-- clickhouse-local, both names"
$CLICKHOUSE_LOCAL --allow_experimental_analyzer 0 --query "SELECT toUInt8(getSetting('allow_experimental_analyzer'))"
$CLICKHOUSE_LOCAL --enable_analyzer 0 --query "SELECT toUInt8(getSetting('enable_analyzer'))"

# A query another server sent is not checked against the constraints either, and the query kind that
# identifies such a query is something a client can declare for itself. That value used to be kept,
# so that the servers of a cluster agree on how one query is analyzed; with the old query analysis
# gone there is nothing left to agree on, and it is ignored like any other.
echo "-- declaring a secondary query"
$CLICKHOUSE_LOCAL --query_kind secondary_query --allow_experimental_analyzer 0 --query "SELECT toUInt8(getSetting('allow_experimental_analyzer'))"
$CLICKHOUSE_LOCAL --query_kind secondary_query --enable_analyzer 0 --query "SELECT toUInt8(getSetting('enable_analyzer'))"
$CLICKHOUSE_CLIENT --query_kind secondary_query --allow_experimental_analyzer 0 --query "SELECT toUInt8(getSetting('allow_experimental_analyzer'))"
$CLICKHOUSE_CLIENT --query_kind secondary_query --enable_analyzer 0 --query "SELECT toUInt8(getSetting('enable_analyzer'))"

# A change sent to the server as an ordinary query is refused rather than ignored, a command-line one
# included.
echo "-- clickhouse-client"
$CLICKHOUSE_CLIENT --allow_experimental_analyzer 0 --query "SELECT 1" 2>&1 | grep -o -m1 'SETTING_CONSTRAINT_VIOLATION'

# A `SETTINGS` clause nested in a subquery is not seen by the constraints either - the client applies
# only the top-level one to its own session - so the server refuses it when it parses the query.
echo "-- nested SETTINGS clause"
$CLICKHOUSE_CLIENT --query "SELECT * FROM (SELECT 1 SETTINGS enable_analyzer = 0)" 2>&1 | grep -o -m1 'INCORRECT_QUERY'
$CLICKHOUSE_CLIENT --query "SELECT * FROM (SELECT 1 SETTINGS allow_experimental_analyzer = 0)" 2>&1 | grep -o -m1 'INCORRECT_QUERY'
$CLICKHOUSE_CLIENT --query "SELECT * FROM (SELECT 1 SETTINGS enable_analyzer = 1)"
