#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-debug, no-asan, no-msan, no-tsan, no-ubsan
# The test needs more than 2 GiB of values in a single batch, so it is heavy on memory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="$CLICKHOUSE_TMP/${CLICKHOUSE_DATABASE}_wide_values.parquet"
trap 'rm -f "$FILE"' EXIT

# A batch holds `output_format_parquet_batch_size` rows whatever they weigh, so 1024 rows of ~2.1 MB
# used to reach the writer as one batch of more than 2 GiB. The values have to be distinct, or the
# dictionary collapses them and nothing grows (`repeat` caps at 1000000 repetitions, hence the
# concatenation). `BYTE_ARRAY` goes to arrow's `BinaryBuilder`, which addresses its data with
# 32-bit offsets, and the page's own size is 32-bit as well.
$CLICKHOUSE_LOCAL --max_memory_usage 0 --query "
    WITH repeat('x', 1000000) || repeat('x', 1000000) || repeat('x', 100000) AS wide
    SELECT number AS n, concat(toString(number), wide) AS s
    FROM numbers(1024)
    FORMAT Parquet
" > "$FILE"

$CLICKHOUSE_LOCAL --max_memory_usage 0 --query "
    SELECT count(), sum(n), sum(length(s)), uniqExact(cityHash64(s)) FROM file('$FILE', Parquet)
"

# `FIXED_LEN_BYTE_ARRAY` shares that builder, so a wide `FixedString` has to be split the same way.
# `toFixedString` pads the number with zero bytes, which keeps the values distinct without first
# building a `String` column of the same 2 GiB next to the `FixedString` one.
$CLICKHOUSE_LOCAL --max_memory_usage 0 --allow_suspicious_fixed_string_types 1 --query "
    SELECT toFixedString(toString(number), 2100016) AS s
    FROM numbers(1024)
    FORMAT Parquet
" > "$FILE"

$CLICKHOUSE_LOCAL --max_memory_usage 0 --query "
    SELECT count(), sum(length(s)), uniqExact(cityHash64(s)) FROM file('$FILE', Parquet)
"

# A record is kept whole so that pages start where the page index says they do, and page indexes are
# on by default. One `Array(String)` row larger than a page has to be split regardless, which is the
# only case that gives up the index. The values need not be distinct here: the split counts the
# bytes the record occupies, whether or not the dictionary would fold them together. Parallel encoding
# would hand the encoder a copy of the whole column, doubling the memory the test needs.
$CLICKHOUSE_LOCAL --max_memory_usage 0 --output_format_parquet_parallel_encoding 0 --query "
    SELECT arrayMap(i -> repeat('y', 1000), range(2200000)) AS a
    FORMAT Parquet
" > "$FILE"

$CLICKHOUSE_LOCAL --max_memory_usage 0 --query "
    SELECT length(a), arraySum(x -> length(x), a) FROM file('$FILE', Parquet)
"

# `FixedString` written as `BYTE_ARRAY` rather than `FIXED_LEN_BYTE_ARRAY`: that encoding prefixes
# every value with its 4-byte length, so the batch is budgeted against what it writes, not just the
# payload.
$CLICKHOUSE_LOCAL --max_memory_usage 0 --allow_suspicious_fixed_string_types 1 --query "
    SELECT toFixedString(concat(toString(number), repeat('z', 70000)), 70008) AS s
    FROM numbers(1024)
    SETTINGS output_format_parquet_fixed_string_as_fixed_byte_array = 0
    FORMAT Parquet
" > "$FILE"

$CLICKHOUSE_LOCAL --max_memory_usage 0 --query "
    SELECT count(), sum(length(s)), uniqExact(cityHash64(s)) FROM file('$FILE', Parquet)
"
