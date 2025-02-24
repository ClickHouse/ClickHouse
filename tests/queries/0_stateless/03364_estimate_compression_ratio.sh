#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

run_mode="${1:-CI}"
column_names=("number_col" "str_col" "array_col" "nullable_col" "sparse_col" "tuple_col")
table_name="table_for_estimate_compression_ratio"
tolerance=0.25
formatted_tolerance=$(echo "scale=2; $tolerance * 100" | bc)%

# all combinations of the following should be tested
codecs=("ZSTD" "LZ4")
block_sizes=(65536 1048576)
num_rows_list=(250_000 500_000)


log_if_debug() {
    if [ "$run_mode" != "CI" ]; then
        echo "$@"
    fi
}

test_compression_ratio() {
    local codec=$1
    local block_size=$2
    local num_rows=$3

    log_if_debug "Testing with codec=$codec, block_size=$block_size, num_rows=$num_rows"

    query="DROP TABLE IF EXISTS $table_name"
    log_if_debug "Running: $query"
    $CLICKHOUSE_CLIENT -q "$query"

    query="CREATE TABLE $table_name (
        number_col UInt64 CODEC($codec),
        str_col String CODEC($codec), 
        array_col Array(String) CODEC($codec),
        nullable_col Nullable(Int64) CODEC($codec),
        sparse_col Int64 CODEC($codec),
        tuple_col Tuple(Int64, String) CODEC($codec)
    ) ENGINE = MergeTree ORDER BY number_col
    SETTINGS min_compress_block_size = $block_size,
             max_compress_block_size = $block_size"

    log_if_debug "Running: $query"
    $CLICKHOUSE_CLIENT -q "$query"

    query="INSERT INTO $table_name 
    SELECT 
        number+rand() as number_col,
        toString(number+rand()) as str_col,
        [toString(number+rand()), toString(number+rand())] as array_col,
        if(number % 20 = 0, number+rand(), NULL) as nullable_col,
        if(number % 100 = 0, number+rand(), 0) as sparse_col,
        (number+rand(), toString(number+rand())) as tuple_col
    FROM system.numbers LIMIT $num_rows"

    log_if_debug "Running: $query"
    $CLICKHOUSE_CLIENT -q "$query"

    query="OPTIMIZE TABLE $table_name FINAL"
    log_if_debug "Running: $query"
    $CLICKHOUSE_CLIENT --query="$query"

    # Estimated compression ratios
    query="SELECT 
        estimateCompressionRatio('$codec', $block_size)(number_col),
        estimateCompressionRatio('$codec', $block_size)(str_col),
        estimateCompressionRatio('$codec', $block_size)(array_col),
        estimateCompressionRatio('$codec', $block_size)(nullable_col),
        estimateCompressionRatio('$codec', $block_size)(sparse_col),
        estimateCompressionRatio('$codec', $block_size)(tuple_col)
    FROM $table_name
    FORMAT TSV"

    log_if_debug "Running: $query"
    ratios=$($CLICKHOUSE_CLIENT -q "$query")

    declare -A estimated_ratios
    i=1
    for col in "${column_names[@]}"; do
        estimated_ratios[$col]=$(echo "$ratios" | cut -f$i)
        ((i++))
    done

    # Check compression ratios
    query="
    SELECT 
        name,
        sum(data_uncompressed_bytes) / sum(data_compressed_bytes) AS ratio
    FROM system.columns
    WHERE table = '$table_name'
        AND database = currentDatabase()
    GROUP BY name
    ORDER BY name ASC
    FORMAT TSV"

    log_if_debug "Running: $query"
    actual_ratios=$($CLICKHOUSE_CLIENT -q "$query")

    failures=0

    for col in "${column_names[@]}"; do
        actual_ratio=$(echo "$actual_ratios" | grep "$col" | cut -f2)
        min_ratio=$(echo "$actual_ratio * (1 - $tolerance)" | bc)
        max_ratio=$(echo "$actual_ratio * (1 + $tolerance)" | bc)
        estimated_ratio=${estimated_ratios[$col]}
        percentage_difference=$(echo "scale=2; 100 * ($estimated_ratio - $actual_ratio) / $actual_ratio" | bc)

        log_if_debug "$col Actual compression ratio: $actual_ratio, estimated compression ratio: $estimated_ratio, tolerance range: $min_ratio - $max_ratio, percentage difference: $percentage_difference%"

        if (( $(echo "$estimated_ratio < $min_ratio || $estimated_ratio > $max_ratio" | bc -l) )); then
            echo "With codec=$codec block_size=$block_size num_rows=$num_rows:"
            echo "Estimated $col compression ratio $estimated_ratio is not within tolerance range compared to actual ratio ($actual_ratio +/- $formatted_tolerance), the percentage difference is $percentage_difference%"
            ((failures++))
        fi
    done

    if ((failures > 0)); then
        exit 1
    fi

    echo "OK - For codec=$codec block_size=$block_size num_rows=$num_rows: Estimated compression ratios are within $formatted_tolerance of actual ratios"
}

for codec in "${codecs[@]}"; do
    for block_size in "${block_sizes[@]}"; do
        for num_rows in "${num_rows_list[@]}"; do
            test_compression_ratio "$codec" "$block_size" "$num_rows"
        done
    done
done
