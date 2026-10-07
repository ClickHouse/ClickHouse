#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/pull/101143#issuecomment-4455999222
#
# The unquoted `kql(...)` table-function parser walks the outer SQL `Tokens` to
# balance parentheses and extract the argument substring. When the argument
# contains a top-level `;` (for example, after a malformed `$$` heredoc such as
# `$$Cust;\nhJSON, Stri`), the walk crossed the semicolon and advanced the
# outer `Tokens`'s high-water mark past the end of the current SQL statement.
# `tryParseQuery` then computed `this_query_end_pos` by walking forward to the
# first semicolon, leaving `last_token.begin > this_query_end_pos->end`. The
# error formatter computed `total_bytes = end - last_token.begin`, underflowed
# `size_t`, and `UTF8::computeBytesBeforeWidth` read far past the buffer.
# ASan reports a heap-buffer-overflow in `computeWidthImpl`; release builds
# silently leak bytes from neighboring heap memory into the error message.
#
# The fix stops the unquoted paren-balancing walk in
# `ParserKQLTableFunction::parseImpl` at any `Semicolon` token. Callers that
# legitimately need KQL `let` statements with `;` must quote the argument with
# `'...'` or `$$...$$` (issue #61742).
#
# Input below was found by `select_parser_fuzzer`
# (crash hash `54f737b6a7b9d6c3a2a7cd333df208dac4eb0361`).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The query is intentionally malformed: an unterminated `$$` heredoc whose
# stand-in payload contains a semicolon embedded inside a `kql(...)` call.
# The parser must surface a syntax error pointing at the embedded `;`,
# without an ASan heap-buffer-overflow.
$CLICKHOUSE_LOCAL --query "$(printf 'SELECT tU from kql($$Cust;\nhJSON, Stri)2() IN (SELECT ers00)\n')" 2>&1 \
    | grep -c -E 'Syntax error|Cannot parse|expected' || true
