#!/usr/bin/env bash

# Regression test for issue #101509.
#
# When `clickhouse-local --ignore-error` runs a multi-statement input and one
# of the statements has a lexical error or unmatched parentheses, the error
# message used to include every subsequent statement of the input (because
# the error formatter received `all_queries_end` instead of the current
# statement's end). For an N-statement script of identical malformed queries,
# this produced O(N^2) error output instead of O(N).
#
# Three error paths in `parseQuery.cpp` were affected:
#   1. Lookahead lexical error (`lookahead->isError()`)
#   2. Post-parse lexical error (`last_token.isError()`)
#   3. Unmatched parentheses (`checkUnmatchedParentheses`)
#
# This test exercises each path by feeding 10 identical malformed statements
# and asserting that the offending text appears exactly 10 times in the
# error output (not 10 + 9 + ... + 1 = 55).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Print a string N times. We avoid `yes` because it produces unbounded output
# that can fill pipe buffers if the consumer is slow.
repeat10()
{
    for _ in {1..10}; do
        printf '%s\n' "$1"
    done
}

# Each line below labels which of the three error paths is being exercised so
# that a CI diff immediately shows the regressing path (otherwise three
# identical 10s on failure would all look the same). Expected count: 10.
# Pre-fix count: 55 (10 + 9 + ... + 1).

# Path 1: early-lookahead lexical error.
# `!` is an invalid character that the lookahead loop in `tryParseQuery`
# detects before the main parser runs.
echo "lookahead-lexical $(repeat10 'SELECT 1 !;' | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -cF 'SELECT 1')"

# Path 2: post-parse lexical error.
# `INSERT INTO` skips the lookahead shortcut (because INSERT queries may
# legitimately contain non-SQL bytes in their VALUES payload), so the lexical
# error is only detected after the main parser has run and consults
# `last_token.isError()`.
echo "post-parse-lexical $(repeat10 'INSERT INTO t SELECT 1 !;' | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -cF 'INSERT INTO t SELECT')"

# Path 3: unmatched parentheses.
# The bare `SELECT` substring is used here (rather than `SELECT (1`) because
# `clickhouse-local` wraps the offending `(` in ANSI highlight codes inside
# the error output, which would split `SELECT (1` across the codes and
# defeat a substring grep.
echo "unmatched-parens $(repeat10 'SELECT (1;' | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -cF 'SELECT')"

# Path 1b: `max_query_size` exceeded. This is the same lookahead lexical path as
# Path 1, but the lexer reports `ErrorMaxQuerySizeExceeded` rather than a per-
# character lexical error. Every subsequent `nextToken` call past the boundary
# is overridden to the same error type, so the scoping loop must stop on
# `isError()`; otherwise it walks forever and the server appears hung (issue
# surfaced as "Server died" in `Fast test` on the first scoping attempt).
# Expect: a single error message, not a hang. We only check that the command
# completes (under the test runner's per-test timeout) and emits one error.
echo "max-query-size $(${CLICKHOUSE_LOCAL} --max_query_size=20 --query "SELECT 'ыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыыы'" 2>&1 | grep -cF 'Max query size exceeded')"

# Sanity: 10 valid statements must not produce any error lines. This guards
# against the fix accidentally rejecting good input (e.g. a regression that
# misidentifies a non-error token as an error). `^Code:` is the prefix of
# every reported `clickhouse-local` error.
echo "no-false-positives $(repeat10 'SELECT 1;' | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -cE '^Code:')"
