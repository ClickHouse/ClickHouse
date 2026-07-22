#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `--send_logs_level=fatal` keeps the server-side log of the exception out of
# stderr, so we only have to count the single client-side error string.
# `--enable_analyzer=1` forces the new analyzer: the hint is implemented in
# `QueryAnalyzer.cpp` and is not produced by the legacy analyzer.

CLIENT="$CLICKHOUSE_CLIENT --enable_analyzer=1 --send_logs_level=fatal"

check_hint() {
    if grep -qF 'Did you forget to add it?'; then
        echo "hint shown"
    else
        echo "no hint"
    fi
}

# Query without FROM clause should hint that FROM is missing.
$CLIENT -q "SELECT x" 2>&1 | check_hint

# Query with multiple unresolved identifiers but no FROM should also hint.
$CLIENT -q "SELECT check_name, count(), sum(dur) AS t WHERE name = 'test' GROUP BY check_name ORDER BY t DESC" 2>&1 | check_hint

# Query with FROM should NOT hint about missing FROM.
$CLIENT -q "SELECT nonexistent_column FROM system.one" 2>&1 | check_hint

# When `implicit_table_at_top_level` is set, the analyzer substitutes that
# table for a missing FROM, so the hint should NOT appear.
$CLIENT --implicit_table_at_top_level=system.one -q "SELECT nonexistent_column" 2>&1 | check_hint

# `implicit_table_at_top_level` only changes the top-level SELECT; subqueries
# still fall back to `system.one`, so the hint should still appear there.
$CLIENT --implicit_table_at_top_level=system.one -q "SELECT (SELECT nonexistent_column)" 2>&1 | check_hint
