#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Generate exactly 10 copies of a query (avoid `yes` which can produce unbounded data in pipe buffers).
repeat10() { for _ in {1..10}; do printf '%s\n' "$1"; done; }

# Lexical errors (e.g. '!' character) should not print all subsequent queries in the error message:
repeat10 'SELECT 1 !;' | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -cF 'SELECT 1'

# Post-parse lexical errors (INSERT-prefix queries skip the early lookahead, so lexical errors
# are caught by the post-parse last_token.isError() branch) should also be bounded:
repeat10 'INSERT INTO t SELECT 1 !;' | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -cF 'INSERT'

# Unmatched parentheses should also not print all subsequent queries in the error message:
repeat10 'SELECT (1;' | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -cF 'SELECT'
