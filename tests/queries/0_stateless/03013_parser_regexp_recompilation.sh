#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This query is incorrect, but its parsing should not be slow (tens of seconds):

${CLICKHOUSE_LOCAL} < "$CURDIR"/03013_parser_regexp_recompilation.txt 2>&1 | grep --text -o -F 'SYNTAX_ERROR'
