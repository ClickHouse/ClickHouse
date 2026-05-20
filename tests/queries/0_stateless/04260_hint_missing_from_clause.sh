#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `--send_logs_level=fatal` keeps the server-side log of the exception out of
# stderr, so we only have to count the single client-side error string.

check_hint() {
    if grep -qF 'Did you forget to add it?'; then
        echo "hint shown"
    else
        echo "no hint"
    fi
}

# Query without FROM clause should hint that FROM is missing.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "SELECT x" 2>&1 | check_hint

# Query with multiple unresolved identifiers but no FROM should also hint.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "SELECT check_name, count(), sum(dur) AS t WHERE name = 'test' GROUP BY check_name ORDER BY t DESC" 2>&1 | check_hint

# Query with FROM should NOT hint about missing FROM.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "SELECT nonexistent_column FROM system.one" 2>&1 | check_hint

# When `implicit_table_at_top_level` is set, the analyzer substitutes that
# table for a missing FROM, so the hint should NOT appear.
$CLICKHOUSE_CLIENT --send_logs_level=fatal --implicit_table_at_top_level=system.one -q "SELECT nonexistent_column" 2>&1 | check_hint
