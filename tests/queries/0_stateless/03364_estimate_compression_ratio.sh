#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

column_names=("number_col" "str_col" "array_col" "nullable_col" "sparse_col" "tuple_col")
table_name="table_for_estimate_compression_ratio"
tolerance=0.30
formatted_tolerance=$(awk "BEGIN {printf \"%.2f\", $tolerance * 100}")%

# all combinations of the following should be tested
codecs=("ZSTD(1)" "LZ4")
block_sizes=(65536 1048576)
num_rows_list=(1_000 50_000)

create_table() {
    local num_rows=$1

    query="DROP TABLE IF EXISTS $table_name"
    $CLICKHOUSE_CLIENT -q "$query"

    query="CREATE TABLE $table_name (
        number_col UInt64,
        str_col String,
        array_col Array(String),
        nullable_col Nullable(Int64),
        sparse_col Int64,
        tuple_col Tuple(Int64, Tuple(Int64, Int64)),
    ) ENGINE = MergeTree ORDER BY number_col
    SETTINGS min_bytes_for_wide_part = 0"

    $CLICKHOUSE_CLIENT -q "$query"

    query="INSERT INTO $table_name 
    SELECT 
        number+rand() as number_col,
        toString(number+rand()) as str_col,
        [toString(number+rand()), toString(number+rand())] as array_col,
        if(number % 20 = 0, number+rand(), NULL) as nullable_col,
        if(number % 3 = 0, number+rand(), 0) as sparse_col,
        (number+rand(), (0, 0)) as tuple_col
    FROM system.numbers LIMIT $num_rows"

    $CLICKHOUSE_CLIENT -q "$query"
}

apply_codec() {
    local codec=$1
    local block_size=$2

    query="ALTER TABLE $table_name MODIFY COLUMN number_col CODEC($codec),
           MODIFY COLUMN str_col CODEC($codec),
           MODIFY COLUMN array_col CODEC($codec),
           MODIFY COLUMN nullable_col CODEC($codec),
           MODIFY COLUMN sparse_col CODEC($codec),
           MODIFY COLUMN tuple_col CODEC($codec)
           SETTINGS min_compress_block_size = $block_size,
                    max_compress_block_size = $block_size"

    $CLICKHOUSE_CLIENT -q "$query"

    query="OPTIMIZE TABLE $table_name FINAL"
    $CLICKHOUSE_CLIENT -q "$query"
}

test_compression_ratio() {
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

    actual_ratios=$($CLICKHOUSE_CLIENT -q "$query")

    failures=0

    for col in "${column_names[@]}"; do
        actual_ratio=$(echo "$actual_ratios" | grep "$col" | cut -f2)
        min_ratio=$(awk "BEGIN {print $actual_ratio * (1 - $tolerance)}")
        max_ratio=$(awk "BEGIN {print $actual_ratio * (1 + $tolerance)}")
        estimated_ratio=${estimated_ratios[$col]}
        percentage_difference=$(awk "BEGIN {printf \"%.2f\", 100 * ($estimated_ratio - $actual_ratio) / $actual_ratio}")

        if (( $(awk "BEGIN {print ($estimated_ratio < $min_ratio || $estimated_ratio > $max_ratio) ? 1 : 0}") )); then
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

for num_rows in "${num_rows_list[@]}"; do
    create_table "$num_rows"
    for codec in "${codecs[@]}"; do
        for block_size in "${block_sizes[@]}"; do
            apply_codec "$codec" "$block_size"
            test_compression_ratio
        done
    done
done
