#!/usr/bin/env bash

# Regression test for the user-visible bug fixed by this PR:
#
#   Parser error messages for multi-statement input previously included the
#   text of every subsequent statement in the error output. Lexical errors
#   (both the early-lookahead path and the post-parse `last_token.isError()`
#   path) and unmatched parentheses now scope the formatted error to only
#   the current statement boundary.
#
# This test verifies the fix by feeding 10 identical bad statements through
# clickhouse-local --ignore-error and asserting that each error message
# mentions the offending query exactly once (10 lines total), not all
# remaining queries (which would yield 10+9+...+1 = 55 lines).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Generate exactly 10 copies of a query (avoid `yes` which can produce unbounded data in pipe buffers).
repeat10() { for _ in {1..10}; do printf '%s\n' "$1"; done; }

# Path 1: early-lookahead lexical error (parseQuery.cpp `if (lookahead->isError())`).
# `!` is an invalid character that is detected before the main parser runs.
repeat10 'SELECT 1 !;' | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -cF 'SELECT 1'

# Path 2: post-parse lexical error (parseQuery.cpp `if (last_token.isError())`).
# INSERT-prefix queries bypass the early lookahead, so the lexical error is
# only surfaced after the main parser has run.
repeat10 'INSERT INTO t SELECT 1 !;' | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -cF 'INSERT'

# Path 3: unmatched parentheses (parseQuery.cpp `checkUnmatchedParentheses`).
repeat10 'SELECT (1;' | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -cF 'SELECT'
