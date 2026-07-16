#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# By default, in batch mode queries are not echoed and the query_id is not printed.
echo '--- default ---'
${CLICKHOUSE_CLIENT} --query="select 1+1"

# --echo echoes the query verbatim (not formatted).
echo '--- echo ---'
${CLICKHOUSE_CLIENT} --echo --query="select 1+1"

# --echo can be explicitly disabled with a boolean value.
echo '--- echo=false ---'
${CLICKHOUSE_CLIENT} --echo=false --query="select 1+1"

# --echo-formatted formats the echoed query.
echo '--- echo-formatted ---'
${CLICKHOUSE_CLIENT} --echo --echo-formatted --query="select 1+1"

# --echo-formatted can be disabled even when --echo is on.
echo '--- echo --echo-formatted=false ---'
${CLICKHOUSE_CLIENT} --echo --echo-formatted=false --query="select 1+1"

# --echo-query-id prints the query_id; the value is non-deterministic so we only count the lines.
echo '--- echo-query-id (count) ---'
${CLICKHOUSE_CLIENT} --echo-query-id --query="select 1+1" 2>&1 | grep -c '^Query id:'

# --verbose does not imply query echoing in clickhouse-client (only the result is printed).
echo '--- client: verbose does not echo ---'
${CLICKHOUSE_CLIENT} --verbose --query="select 1+1" 2>/dev/null

# --verbose implies query echoing in clickhouse-local (historical behavior).
echo '--- local: verbose echoes ---'
${CLICKHOUSE_LOCAL} --verbose --query="select 1+1" 2>/dev/null

# A user-fixed --query_id must be preserved (printed before execution and used for execution),
# including in clickhouse-local. See https://github.com/ClickHouse/ClickHouse/pull/106191.
echo '--- local: fixed query_id is preserved with echo-query-id ---'
${CLICKHOUSE_LOCAL} --query_id="04308_fixed_id" --echo-query-id --query="SELECT currentQueryID()"
