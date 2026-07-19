#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY='FROM numbers(3) |> WHERE number > 0 |> ORDER BY number'

echo '-- clickhouse-format without the setting rejects pipe syntax'
$CLICKHOUSE_FORMAT --query "$QUERY" 2>&1 | grep -c 'SYNTAX_ERROR'

echo '-- clickhouse-format with --allow_experimental_pipe_syntax'
$CLICKHOUSE_FORMAT --allow_experimental_pipe_syntax=1 --query "$QUERY"

echo '-- clickhouse-format --oneline with --allow_experimental_pipe_syntax'
$CLICKHOUSE_FORMAT --oneline --allow_experimental_pipe_syntax=1 --query "$QUERY"

echo '-- formatQuery honors the setting'
$CLICKHOUSE_CLIENT --query "SELECT formatQuerySingleLine('$QUERY') SETTINGS allow_experimental_pipe_syntax = 1"
$CLICKHOUSE_CLIENT --query "SELECT formatQueryOrNull('$QUERY') SETTINGS allow_experimental_pipe_syntax = 0"

echo '-- highlightQuery honors the setting'
$CLICKHOUSE_CLIENT --query "SELECT highlightQuery('$QUERY') SETTINGS allow_experimental_pipe_syntax = 1"
$CLICKHOUSE_CLIENT --query "SELECT highlightQuery('$QUERY') SETTINGS allow_experimental_pipe_syntax = 0"
