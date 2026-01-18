#!/usr/bin/env bash
# Benchmark script for Avro parallel parsing performance.
# NOT a test - meant for manual performance evaluation.
# Run with: ./03786_avro_parallel_parsing_benchmark.sh
#
# Tags: no-fasttest, no-parallel, disabled

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Benchmark configuration
WARMUP_RUNS=2
BENCHMARK_RUNS=5
ROWS_SMALL=100000
ROWS_MEDIUM=1000000
ROWS_LARGE=5000000

AVRO_FILE="${CLICKHOUSE_TMP}/avro_benchmark_${CLICKHOUSE_DATABASE}.avro"

echo "========================================"
echo "Avro Parallel Parsing Benchmark"
echo "========================================"
echo ""
echo "Configuration:"
echo "  Warmup runs: $WARMUP_RUNS"
echo "  Benchmark runs: $BENCHMARK_RUNS"
echo "  Test sizes: $ROWS_SMALL / $ROWS_MEDIUM / $ROWS_LARGE rows"
echo ""

run_benchmark() {
    local label="$1"
    local parallel="$2"
    local threads="$3"
    local query="$4"
    local runs="$5"

    local total_time=0
    local times=()

    for ((i=1; i<=runs; i++)); do
        # Clear caches
        ${CLICKHOUSE_CLIENT} -q "SYSTEM DROP FILESYSTEM CACHE" 2>/dev/null || true

        # Run query and capture time
        local start_time=$(date +%s%N)
        ${CLICKHOUSE_CLIENT} \
            --input_format_avro_parallel_parsing=$parallel \
            --max_threads=$threads \
            -q "$query" > /dev/null
        local end_time=$(date +%s%N)

        local elapsed=$(( (end_time - start_time) / 1000000 ))  # ms
        times+=($elapsed)
        total_time=$((total_time + elapsed))
    done

    local avg_time=$((total_time / runs))

    # Calculate min/max
    local min_time=${times[0]}
    local max_time=${times[0]}
    for t in "${times[@]}"; do
        ((t < min_time)) && min_time=$t
        ((t > max_time)) && max_time=$t
    done

    printf "  %-35s avg: %6d ms  (min: %d, max: %d)\n" "$label" $avg_time $min_time $max_time
    echo $avg_time  # Return value for speedup calculation
}

benchmark_file() {
    local rows="$1"
    local label="$2"

    echo ""
    echo "--- $label ($rows rows) ---"

    # Generate test file
    echo "Generating Avro file..."
    ${CLICKHOUSE_CLIENT} -q "
        SELECT
            number as id,
            concat('user_', toString(number)) as name,
            number % 1000 as department,
            toFloat64(number) * 1.5 as salary,
            toString(toDate('2020-01-01') + number % 1000) as hire_date
        FROM numbers($rows)
        FORMAT Avro
    " > "${AVRO_FILE}"

    local file_size=$(stat -c%s "${AVRO_FILE}" 2>/dev/null || stat -f%z "${AVRO_FILE}")
    echo "File size: $(echo "scale=2; $file_size / 1048576" | bc) MB"
    echo ""

    local query="SELECT sum(id), count(), avg(salary) FROM file('${AVRO_FILE}', Avro, 'id UInt64, name String, department UInt32, salary Float64, hire_date String')"

    # Warmup
    echo "Warming up..."
    for ((i=1; i<=WARMUP_RUNS; i++)); do
        ${CLICKHOUSE_CLIENT} --input_format_avro_parallel_parsing=0 -q "$query" > /dev/null
        ${CLICKHOUSE_CLIENT} --input_format_avro_parallel_parsing=1 --max_threads=4 -q "$query" > /dev/null
    done

    echo "Benchmarking..."

    # Sequential
    seq_time=$(run_benchmark "Sequential (parallel=0)" 0 1 "$query" $BENCHMARK_RUNS)

    # Parallel with different thread counts
    par1_time=$(run_benchmark "Parallel (threads=1)" 1 1 "$query" $BENCHMARK_RUNS)
    par2_time=$(run_benchmark "Parallel (threads=2)" 1 2 "$query" $BENCHMARK_RUNS)
    par4_time=$(run_benchmark "Parallel (threads=4)" 1 4 "$query" $BENCHMARK_RUNS)
    par8_time=$(run_benchmark "Parallel (threads=8)" 1 8 "$query" $BENCHMARK_RUNS)

    echo ""
    echo "Speedup vs sequential:"
    if [ "$par1_time" -gt 0 ]; then
        printf "  1 thread:  %.2fx\n" $(echo "scale=2; $seq_time / $par1_time" | bc)
    fi
    if [ "$par2_time" -gt 0 ]; then
        printf "  2 threads: %.2fx\n" $(echo "scale=2; $seq_time / $par2_time" | bc)
    fi
    if [ "$par4_time" -gt 0 ]; then
        printf "  4 threads: %.2fx\n" $(echo "scale=2; $seq_time / $par4_time" | bc)
    fi
    if [ "$par8_time" -gt 0 ]; then
        printf "  8 threads: %.2fx\n" $(echo "scale=2; $seq_time / $par8_time" | bc)
    fi
}

# Run benchmarks at different sizes
benchmark_file $ROWS_SMALL "Small"
benchmark_file $ROWS_MEDIUM "Medium"
benchmark_file $ROWS_LARGE "Large"

# Cleanup
rm -f "${AVRO_FILE}"

echo ""
echo "========================================"
echo "Benchmark complete"
echo "========================================"
