#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In clickhouse-local the session is the invocation: the history accumulates across
# all queries of the invocation, and the reading query sees itself.
${CLICKHOUSE_LOCAL} -q "SELECT 'marker' FORMAT Null; SELECT count(), countIf(query_id = queryID()) FROM system.session_query_ids"
