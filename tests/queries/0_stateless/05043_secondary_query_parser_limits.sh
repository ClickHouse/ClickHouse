#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `query_kind` arrives over the wire and can be set by any native client, so a query that merely claims
# to be a `SECONDARY_QUERY` must not get `max_parser_depth` / `max_parser_backtracks` lifted: a genuine
# distributed fan-out of a server-owned query carries the lifted limits through the ordinary settings
# channel instead (covered by `04897_handler_request_parser_limits`).

DEEP="$(python3 -c "print('identity(' * 50 + '1' + ')' * 50)")"

${CLICKHOUSE_CLIENT} --query_kind secondary_query --max_parser_depth 10 --query "SELECT ${DEEP}" 2>&1 \
    | grep -oF "TOO_DEEP_RECURSION" | head -1

# The limits stay ordinary settings: lifted explicitly, the same query parses.
${CLICKHOUSE_CLIENT} --query_kind secondary_query --max_parser_depth 0 --query "SELECT ${DEEP}"
