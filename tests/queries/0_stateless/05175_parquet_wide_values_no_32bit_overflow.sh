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
    SELECT count(), sum(n), sum(length(s)), uniqExact(s) FROM file('$FILE', Parquet)
"

# `FIXED_LEN_BYTE_ARRAY` shares that builder, so a wide `FixedString` has to be split the same way.
$CLICKHOUSE_LOCAL --max_memory_usage 0 --allow_suspicious_fixed_string_types 1 --query "
    WITH repeat('x', 1000000) || repeat('x', 1000000) || repeat('x', 100000) AS wide
    SELECT toFixedString(concat(toString(number), wide), 2100016) AS s
    FROM numbers(1024)
    FORMAT Parquet
" > "$FILE"

$CLICKHOUSE_LOCAL --max_memory_usage 0 --query "
    SELECT count(), sum(length(s)), uniqExact(s) FROM file('$FILE', Parquet)
"
