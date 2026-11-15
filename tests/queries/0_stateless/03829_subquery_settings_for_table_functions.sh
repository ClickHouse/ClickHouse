#!/usr/bin/env bash
# Tags: no-fasttest

# Verify that per-subquery SETTINGS are applied to table functions
# at different nesting levels.
# https://github.com/ClickHouse/ClickHouse/issues/94639

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Create temporary CSV files with different delimiters.
comma_csv="${CLICKHOUSE_TMP}/test_comma_${CLICKHOUSE_DATABASE}.csv"
pipe_csv="${CLICKHOUSE_TMP}/test_pipe_${CLICKHOUSE_DATABASE}.csv"
cache_csv="${CLICKHOUSE_TMP}/test_cache_${CLICKHOUSE_DATABASE}.csv"
delim_csv="${CLICKHOUSE_TMP}/test_delim_${CLICKHOUSE_DATABASE}.csv"

echo 'a,1' > "$comma_csv"
echo 'b,2' >> "$comma_csv"

echo 'c|3' > "$pipe_csv"
echo 'd|4' >> "$pipe_csv"

printf '1\n2\n3\n' > "$cache_csv"

# A file whose content can be parsed with either comma or pipe delimiter.
echo '1,2|3' > "$delim_csv"

# Test 1: SETTINGS on the immediate subquery level (CTE).
$CLICKHOUSE_LOCAL --query "
    WITH
        file_a AS (SELECT * FROM file('${comma_csv}', CSV, 'name String, value UInt32') SETTINGS format_csv_delimiter = ','),
        file_b AS (SELECT * FROM file('${pipe_csv}', CSV, 'name String, value UInt32') SETTINGS format_csv_delimiter = '|')
    SELECT * FROM (
        SELECT * FROM file_a
        UNION ALL
        SELECT * FROM file_b
    ) ORDER BY name
"

# Test 2: SETTINGS on the immediate subquery level (inline subquery).
$CLICKHOUSE_LOCAL --query "
    SELECT * FROM (SELECT * FROM file('${pipe_csv}', CSV, 'name String, value UInt32') SETTINGS format_csv_delimiter = '|') ORDER BY name
"

# Test 3: SETTINGS on a parent subquery level — the table function is in an inner
# subquery without its own SETTINGS, but the outer subquery has SETTINGS.
$CLICKHOUSE_LOCAL --query "
    SELECT * FROM (
        SELECT * FROM (
            SELECT * FROM file('${pipe_csv}', CSV, 'name String, value UInt32')
        )
        SETTINGS format_csv_delimiter = '|'
    ) ORDER BY name
"

# Test 4: SETTINGS on a grandparent level — two levels above the table function.
$CLICKHOUSE_LOCAL --query "
    SELECT * FROM (
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM file('${pipe_csv}', CSV, 'name String, value UInt32')
            )
        )
        SETTINGS format_csv_delimiter = '|'
    ) ORDER BY name
"

# Test 5: SETTINGS at multiple levels — inner overrides outer.
# The outer subquery sets delimiter to comma, the inner overrides to pipe.
$CLICKHOUSE_LOCAL --query "
    SELECT * FROM (
        SELECT * FROM (
            SELECT * FROM file('${pipe_csv}', CSV, 'name String, value UInt32')
            SETTINGS format_csv_delimiter = '|'
        )
        SETTINGS format_csv_delimiter = ','
    ) ORDER BY name
"

# Test 6: SETTINGS at multiple levels — both CTEs with different delimiters
# and each wraps the table function in an extra subquery layer.
$CLICKHOUSE_LOCAL --query "
    WITH
        file_a AS (SELECT * FROM (SELECT * FROM file('${comma_csv}', CSV, 'name String, value UInt32')) SETTINGS format_csv_delimiter = ','),
        file_b AS (SELECT * FROM (SELECT * FROM file('${pipe_csv}', CSV, 'name String, value UInt32')) SETTINGS format_csv_delimiter = '|')
    SELECT * FROM (
        SELECT * FROM file_a
        UNION ALL
        SELECT * FROM file_b
    ) ORDER BY name
"

# Test 7: SETTINGS applied to file('-') reading from stdin.
echo 'e|5' | $CLICKHOUSE_LOCAL --query "
    SELECT * FROM (SELECT * FROM file('-', CSV, 'name String, value UInt32') SETTINGS format_csv_delimiter = '|')
"

# Test 8: Same file with different format_csv_delimiter in the same query.
# Each branch of the UNION ALL uses its own delimiter on the same file.
# This verifies that different per-subquery settings produce correct results
# within a single query (i.e. the table function results are not incorrectly
# shared across subqueries with different settings).
$CLICKHOUSE_LOCAL --query "
    SELECT a, b FROM (
        SELECT * FROM (SELECT * FROM file('${delim_csv}', CSV, 'a String, b String') SETTINGS format_csv_delimiter = ',')
        UNION ALL
        SELECT * FROM (SELECT * FROM file('${delim_csv}', CSV, 'a String, b String') SETTINGS format_csv_delimiter = '|')
    ) ORDER BY a
"

# Test 9: Verify table function caching — same table function with
# same SETTINGS should be executed only once (cached).
$CLICKHOUSE_LOCAL --query "
    SELECT count() FROM (
        SELECT * FROM (SELECT * FROM file('${cache_csv}', TSV, 'x UInt32') SETTINGS max_block_size = 65505)
        UNION ALL
        SELECT * FROM (SELECT * FROM file('${cache_csv}', TSV, 'x UInt32') SETTINGS max_block_size = 65505)
    );
    SELECT value FROM system.events WHERE event = 'TableFunctionExecute';
"

# Test 10: Different SETTINGS should NOT be cached — each file() table function
# gets a separate execution despite having the same path and schema.
$CLICKHOUSE_LOCAL --query "
    SELECT count() FROM (
        SELECT * FROM (SELECT * FROM file('${cache_csv}', TSV, 'x UInt32') SETTINGS max_block_size = 65505)
        UNION ALL
        SELECT * FROM (SELECT * FROM file('${cache_csv}', TSV, 'x UInt32') SETTINGS max_block_size = 65506)
    );
    SELECT value FROM system.events WHERE event = 'TableFunctionExecute';
"

# Cleanup.
rm -f "$comma_csv" "$pipe_csv" "$cache_csv" "$delim_csv"
