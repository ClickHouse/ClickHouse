#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Valid ARRAY JOIN expression lists (including parenthesized tuples) keep working.
$CLICKHOUSE_CLIENT -q "SELECT a, b FROM system.one ARRAY JOIN [1, 2] AS a, [3, 4] AS b ORDER BY a, b"
$CLICKHOUSE_CLIENT -q "SELECT t.1, t.2 FROM system.one ARRAY JOIN [(1, 2), (3, 4)] AS t ORDER BY t.1, t.2"

# A comma right after ARRAY JOIN belongs to the array join expression list, never a cross
# join. When the item after the comma is not a valid expression but is a valid parenthesized
# table list (here it contains view(SELECT ...)), it must be rejected rather than reinterpreted
# as a comma-joined derived table: that reinterpretation formats back to a plain (SELECT ...)
# subquery the array join then absorbs, so the format/parse round-trip diverges (this triggers
# the Inconsistent AST formatting consistency check in debug builds).
$CLICKHOUSE_CLIENT -q "SELECT * FROM system.one ARRAY JOIN [1] AS a, (b, view(SELECT 1 AS A), x)" 2>&1 | grep -o -m1 "SYNTAX_ERROR"
