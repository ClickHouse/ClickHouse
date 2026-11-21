#!/usr/bin/env bash
set -euo pipefail

BIN="./build_release/programs"
ITERATIONS=10
CONCURRENCY=2
RESULTS_DIR="bench_results"

mkdir -p "$RESULTS_DIR"

TABLES=(
    t64_source
    t64_bit_true_zstd
    t64_bit_false_zstd
    t64_byte_true_zstd
    t64_byte_false_zstd
    t64_bit_true_lz4
    t64_bit_false_lz4
    t64_byte_true_lz4
    t64_byte_false_lz4
)

for tbl in "${TABLES[@]}"; do
    echo "=== $tbl ==="
    "$BIN/clickhouse-benchmark" \
        --iterations "$ITERATIONS" \
        --concurrency "$CONCURRENCY" \
        --delay 0 \
        --query "
          SELECT
            sum(bit_boundaries), avg(bit_boundaries), min(bit_boundaries), max(bit_boundaries),
            sum(sign_flipping),  avg(sign_flipping),  min(sign_flipping),  max(sign_flipping),
            sum(mid_range),      avg(mid_range),      min(mid_range),      max(mid_range),
            sum(small_range),    avg(small_range),    min(small_range),    max(small_range),
            sum(ts),             avg(ts),             min(ts),             max(ts)
          FROM $tbl" \
        2>&1 | tee "$RESULTS_DIR/${tbl}.txt"
    grep -E "50\.000%|95\.000%|99\.000%" "$RESULTS_DIR/${tbl}.txt" || true
    echo ""
done


