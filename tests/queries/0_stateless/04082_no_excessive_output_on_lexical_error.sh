#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lexical errors (e.g. '!' character) should not print all subsequent queries in the error message:
yes 'SELECT 1 !;' | head -n10 | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -cF 'SELECT 1'

# Post-parse lexical errors (INSERT-prefix queries skip the early lookahead, so lexical errors
# are caught by the post-parse last_token.isError() branch) should also be bounded:
yes 'INSERT INTO t SELECT 1 !;' | head -n10 | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -c 'INSERT'

# Unmatched parentheses should also not print all subsequent queries in the error message:
yes 'SELECT (1;' | head -n10 | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -c 'SELECT'
