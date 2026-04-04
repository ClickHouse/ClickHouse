#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lexical errors (e.g. '!' character) should not print all subsequent queries in the error message:
yes 'SELECT 1 !;' | head -n10 | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -F 'SELECT 1' | wc -l

# Unmatched parentheses should also not print all subsequent queries in the error message:
yes 'SELECT (1;' | head -n10 | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -c 'SELECT'
