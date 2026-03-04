#!/bin/bash
#
# Text Index Apply Mode Benchmark Script
# Usage: ./run_text_index_benchmark.sh [clickhouse-client-path] [iterations]
#
# This script benchmarks three modes:
# - lazy: Compressed posting lists with lazy cursor-based decoding
# - materialize: Compressed posting lists with full materialization
# - uncompressed: Traditional roaring bitmap
#

set -e

CLICKHOUSE_CLIENT="${1:-clickhouse-client}"
ITERATIONS="${2:-5}"
ROWS=1000000

echo "=========================================="
echo "Text Index Apply Mode Benchmark"
echo "=========================================="
echo "ClickHouse client: $CLICKHOUSE_CLIENT"
echo "Iterations per query: $ITERATIONS"
echo "Total rows: $ROWS"
echo ""

# Cleanup function
cleanup() {
    echo "Cleaning up tables..."
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS text_index_lazy"
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS text_index_materialize"
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS text_index_uncompressed"
}

# Setup tables
setup() {
    echo "Creating tables..."

    # LAZY mode
    $CLICKHOUSE_CLIENT --query "
    CREATE TABLE text_index_lazy (
        id UInt64,
        text String,
        INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', posting_list_apply_mode = 'lazy')
    ) ENGINE = MergeTree ORDER BY id
    "

    # MATERIALIZE mode
    $CLICKHOUSE_CLIENT --query "
    CREATE TABLE text_index_materialize (
        id UInt64,
        text String,
        INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', posting_list_apply_mode = 'materialize')
    ) ENGINE = MergeTree ORDER BY id
    "

    # UNCOMPRESSED mode
    $CLICKHOUSE_CLIENT --query "
    CREATE TABLE text_index_uncompressed (
        id UInt64,
        text String,
        INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha')
    ) ENGINE = MergeTree ORDER BY id
    "

    echo "Inserting data (this may take a while)..."

    # Insert data with various token distributions
    $CLICKHOUSE_CLIENT --query "
    INSERT INTO text_index_lazy
    SELECT
        number AS id,
        concat(
            if(rand(number) % 1000 = 0, 'rare_alpha ', ''),
            if(rand(number + 1) % 1000 = 1, 'rare_beta ', ''),
            if(rand(number + 2) % 1000 = 2, 'rare_gamma ', ''),
            if(rand(number + 3) % 1000 = 3, 'rare_delta ', ''),
            if(rand(number + 10) % 20 = 0, 'medium_one ', ''),
            if(rand(number + 11) % 20 = 1, 'medium_two ', ''),
            if(rand(number + 12) % 20 = 2, 'medium_three ', ''),
            if(rand(number + 13) % 20 = 3, 'medium_four ', ''),
            if(rand(number + 20) % 10 < 3, 'frequent_x ', ''),
            if(rand(number + 21) % 10 < 3, 'frequent_y ', ''),
            if(rand(number + 22) % 10 < 3, 'frequent_z ', ''),
            if(rand(number + 30) % 5 < 4, 'common ', ''),
            'text_content_', toString(number % 100)
        ) AS text
    FROM numbers($ROWS)
    "

    $CLICKHOUSE_CLIENT --query "INSERT INTO text_index_materialize SELECT * FROM text_index_lazy"
    $CLICKHOUSE_CLIENT --query "INSERT INTO text_index_uncompressed SELECT * FROM text_index_lazy"

    echo "Data insertion complete."
    echo ""
}

# Run a single benchmark
run_benchmark() {
    local description="$1"
    local query_lazy="$2"
    local query_materialize="$3"
    local query_uncompressed="$4"

    echo "--- $description ---"

    # Run with timing
    printf "  lazy:        "
    $CLICKHOUSE_CLIENT --time --format Null --query "$query_lazy" 2>&1 | tail -1

    printf "  materialize: "
    $CLICKHOUSE_CLIENT --time --format Null --query "$query_materialize" 2>&1 | tail -1

    printf "  uncompressed:"
    $CLICKHOUSE_CLIENT --time --format Null --query "$query_uncompressed" 2>&1 | tail -1

    echo ""
}

# Run benchmarks using clickhouse-benchmark for more accurate results
run_benchmark_accurate() {
    local description="$1"
    local query_lazy="$2"
    local query_materialize="$3"
    local query_uncompressed="$4"

    echo "--- $description ---"

    printf "  lazy:        "
    echo "$query_lazy" | clickhouse-benchmark -i $ITERATIONS 2>&1 | grep -E "^[0-9]" | head -1

    printf "  materialize: "
    echo "$query_materialize" | clickhouse-benchmark -i $ITERATIONS 2>&1 | grep -E "^[0-9]" | head -1

    printf "  uncompressed:"
    echo "$query_uncompressed" | clickhouse-benchmark -i $ITERATIONS 2>&1 | grep -E "^[0-9]" | head -1

    echo ""
}

SETTINGS="SETTINGS force_data_skipping_indices='idx_text', enable_full_text_index=1, query_plan_direct_read_from_text_index=1"

# Main execution
trap cleanup EXIT

cleanup
setup

echo "=========================================="
echo "Running Benchmarks"
echo "=========================================="
echo ""

echo "=== Group 1: Single Token Queries ==="
run_benchmark "Single rare token (~0.1% rows)" \
    "SELECT count() FROM text_index_lazy WHERE hasToken(text, 'rare_alpha') $SETTINGS" \
    "SELECT count() FROM text_index_materialize WHERE hasToken(text, 'rare_alpha') $SETTINGS" \
    "SELECT count() FROM text_index_uncompressed WHERE hasToken(text, 'rare_alpha') $SETTINGS"

run_benchmark "Single medium token (~5% rows)" \
    "SELECT count() FROM text_index_lazy WHERE hasToken(text, 'medium_one') $SETTINGS" \
    "SELECT count() FROM text_index_materialize WHERE hasToken(text, 'medium_one') $SETTINGS" \
    "SELECT count() FROM text_index_uncompressed WHERE hasToken(text, 'medium_one') $SETTINGS"

run_benchmark "Single frequent token (~30% rows)" \
    "SELECT count() FROM text_index_lazy WHERE hasToken(text, 'frequent_x') $SETTINGS" \
    "SELECT count() FROM text_index_materialize WHERE hasToken(text, 'frequent_x') $SETTINGS" \
    "SELECT count() FROM text_index_uncompressed WHERE hasToken(text, 'frequent_x') $SETTINGS"

run_benchmark "Single common token (~80% rows)" \
    "SELECT count() FROM text_index_lazy WHERE hasToken(text, 'common') $SETTINGS" \
    "SELECT count() FROM text_index_materialize WHERE hasToken(text, 'common') $SETTINGS" \
    "SELECT count() FROM text_index_uncompressed WHERE hasToken(text, 'common') $SETTINGS"

echo "=== Group 2: Union Queries (hasAnyTokens) ==="
run_benchmark "Union 2 rare tokens" \
    "SELECT count() FROM text_index_lazy WHERE hasAnyTokens(text, ['rare_alpha', 'rare_beta']) $SETTINGS" \
    "SELECT count() FROM text_index_materialize WHERE hasAnyTokens(text, ['rare_alpha', 'rare_beta']) $SETTINGS" \
    "SELECT count() FROM text_index_uncompressed WHERE hasAnyTokens(text, ['rare_alpha', 'rare_beta']) $SETTINGS"

run_benchmark "Union 4 medium tokens" \
    "SELECT count() FROM text_index_lazy WHERE hasAnyTokens(text, ['medium_one', 'medium_two', 'medium_three', 'medium_four']) $SETTINGS" \
    "SELECT count() FROM text_index_materialize WHERE hasAnyTokens(text, ['medium_one', 'medium_two', 'medium_three', 'medium_four']) $SETTINGS" \
    "SELECT count() FROM text_index_uncompressed WHERE hasAnyTokens(text, ['medium_one', 'medium_two', 'medium_three', 'medium_four']) $SETTINGS"

run_benchmark "Union 3 frequent tokens" \
    "SELECT count() FROM text_index_lazy WHERE hasAnyTokens(text, ['frequent_x', 'frequent_y', 'frequent_z']) $SETTINGS" \
    "SELECT count() FROM text_index_materialize WHERE hasAnyTokens(text, ['frequent_x', 'frequent_y', 'frequent_z']) $SETTINGS" \
    "SELECT count() FROM text_index_uncompressed WHERE hasAnyTokens(text, ['frequent_x', 'frequent_y', 'frequent_z']) $SETTINGS"

echo "=== Group 3: Intersection Queries (hasAllTokens) ==="
run_benchmark "Intersect 2 rare tokens" \
    "SELECT count() FROM text_index_lazy WHERE hasAllTokens(text, ['rare_alpha', 'rare_beta']) $SETTINGS" \
    "SELECT count() FROM text_index_materialize WHERE hasAllTokens(text, ['rare_alpha', 'rare_beta']) $SETTINGS" \
    "SELECT count() FROM text_index_uncompressed WHERE hasAllTokens(text, ['rare_alpha', 'rare_beta']) $SETTINGS"

run_benchmark "Intersect 2 medium tokens" \
    "SELECT count() FROM text_index_lazy WHERE hasAllTokens(text, ['medium_one', 'medium_two']) $SETTINGS" \
    "SELECT count() FROM text_index_materialize WHERE hasAllTokens(text, ['medium_one', 'medium_two']) $SETTINGS" \
    "SELECT count() FROM text_index_uncompressed WHERE hasAllTokens(text, ['medium_one', 'medium_two']) $SETTINGS"

run_benchmark "Intersect 4 medium tokens" \
    "SELECT count() FROM text_index_lazy WHERE hasAllTokens(text, ['medium_one', 'medium_two', 'medium_three', 'medium_four']) $SETTINGS" \
    "SELECT count() FROM text_index_materialize WHERE hasAllTokens(text, ['medium_one', 'medium_two', 'medium_three', 'medium_four']) $SETTINGS" \
    "SELECT count() FROM text_index_uncompressed WHERE hasAllTokens(text, ['medium_one', 'medium_two', 'medium_three', 'medium_four']) $SETTINGS"

run_benchmark "Intersect 2 frequent tokens" \
    "SELECT count() FROM text_index_lazy WHERE hasAllTokens(text, ['frequent_x', 'frequent_y']) $SETTINGS" \
    "SELECT count() FROM text_index_materialize WHERE hasAllTokens(text, ['frequent_x', 'frequent_y']) $SETTINGS" \
    "SELECT count() FROM text_index_uncompressed WHERE hasAllTokens(text, ['frequent_x', 'frequent_y']) $SETTINGS"

run_benchmark "Intersect rare + frequent (skip-list advantage)" \
    "SELECT count() FROM text_index_lazy WHERE hasAllTokens(text, ['rare_alpha', 'frequent_x']) $SETTINGS" \
    "SELECT count() FROM text_index_materialize WHERE hasAllTokens(text, ['rare_alpha', 'frequent_x']) $SETTINGS" \
    "SELECT count() FROM text_index_uncompressed WHERE hasAllTokens(text, ['rare_alpha', 'frequent_x']) $SETTINGS"

echo "=== Group 4: Edge Cases ==="
run_benchmark "Non-existent token" \
    "SELECT count() FROM text_index_lazy WHERE hasToken(text, 'nonexistent_xyz') $SETTINGS" \
    "SELECT count() FROM text_index_materialize WHERE hasToken(text, 'nonexistent_xyz') $SETTINGS" \
    "SELECT count() FROM text_index_uncompressed WHERE hasToken(text, 'nonexistent_xyz') $SETTINGS"

run_benchmark "Intersect with non-existent token" \
    "SELECT count() FROM text_index_lazy WHERE hasAllTokens(text, ['frequent_x', 'nonexistent_xyz']) $SETTINGS" \
    "SELECT count() FROM text_index_materialize WHERE hasAllTokens(text, ['frequent_x', 'nonexistent_xyz']) $SETTINGS" \
    "SELECT count() FROM text_index_uncompressed WHERE hasAllTokens(text, ['frequent_x', 'nonexistent_xyz']) $SETTINGS"

echo "=========================================="
echo "Benchmark Complete"
echo "=========================================="
