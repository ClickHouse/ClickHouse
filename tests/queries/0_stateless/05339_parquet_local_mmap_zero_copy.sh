#!/usr/bin/env bash
# Tags: no-fasttest
# Parquet reads from memory-mapped files (the clickhouse-local default) must match reads with pread.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$DATA"

COLUMNS="number AS a, toUInt32(number % 100) AS b, toString(number * 7) AS s, toDecimal64(number % 1000, 4) AS d,
    if(number % 5 = 0, NULL, toInt64(number)) AS n, range(number % 4) AS arr, map(toString(number % 3), toUInt8(number % 7)) AS m"

for codec in none zstd; do
    $CLICKHOUSE_LOCAL -q "SELECT $COLUMNS FROM numbers(200000) INTO OUTFILE '$DATA/$codec.parquet' FORMAT Parquet
        SETTINGS output_format_parquet_compression_method = '$codec', output_format_parquet_row_group_size = 10000"
done
$CLICKHOUSE_LOCAL -q "SELECT $COLUMNS FROM numbers(1) INTO OUTFILE '$DATA/one_row.parquet' FORMAT Parquet"
$CLICKHOUSE_LOCAL --storage_file_read_method=mmap --print-profile-events -q "SELECT sum(a) FROM file('$DATA/none.parquet') FORMAT Null" > "$DATA/events.txt" 2>&1
grep -F -q "CreatedReadBufferMMap:" "$DATA/events.txt" && echo "mmap is used" || echo "mmap is NOT used"
grep -F -q "ParquetPrefetcherReadZeroCopy:" "$DATA/events.txt" && echo "zero-copy reads are used" || echo "zero-copy reads are NOT used"

for file in none zstd one_row; do
    for query in \
        "SELECT count(), sum(a), sum(b), sum(length(s)), sum(d), sum(n), sum(arraySum(arr)), sum(cityHash64(a, b, s, d, ifNull(n, -1), arr, m)) FROM file('$DATA/$file.parquet')" \
        "SELECT count(), sum(a), sum(n) FROM file('$DATA/$file.parquet') WHERE b = 42" \
        "SELECT a, s, d, n, arr, m FROM file('$DATA/$file.parquet') ORDER BY s DESC LIMIT 3" \
        "SELECT count() FROM file('$DATA/$file.parquet') WHERE a BETWEEN 12345 AND 98765"
    do
        mmap=$($CLICKHOUSE_LOCAL --storage_file_read_method=mmap -q "$query" 2>&1)
        pread=$($CLICKHOUSE_LOCAL --storage_file_read_method=pread -q "$query" 2>&1)
        echo "$file: $mmap"
        [ "$mmap" == "$pread" ] || echo "mismatch with pread: $pread"
    done
done

rm -rf "$DATA"
