#!/usr/bin/env bash
# Tags: no-fasttest
# Filtering a Parquet file without a page index returns the right rows for columns stored with the
# RLE / bit-packed hybrid encoding: a one-entry dictionary whose indexes are stored with bit width 0,
# as parquet-mr writes it, and an RLE-encoded Bool column, pyarrow's default for data page v2.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SETTINGS="input_format_parquet_max_block_size = 100, input_format_parquet_prefer_block_bytes = 0,
    input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_dictionary_filter_push_down = 0, use_query_condition_cache = 0"

# 1000 rows: k = row number, c = 42.
for f in 'k % 7 = 3' 'k < 1000' 'k BETWEEN 150 AND 160'; do
    $CLICKHOUSE_LOCAL -q "
        SELECT count(), sum(c), sum(k) FROM file('$CURDIR/data_parquet/05318_parquet_dictionary_bit_width_0.parquet')
        PREWHERE $f
        SETTINGS $SETTINGS"
done

# 3000 rows: k = row number, flag = (intDiv(k, 100) % 2 = 0) for k < 1500 (long runs), (k % 3 = 0) after.
BOOL_FILE="$CURDIR/data_parquet/05318_parquet_rle_boolean.parquet"
$CLICKHOUSE_LOCAL -q "
    SELECT tupleElement(arrayJoin(columns) AS col, 'name'), tupleElement(col, 'encodings')
    FROM file('$BOOL_FILE', ParquetMetadata)"
for f in 'k % 7 = 3' 'k % 997 = 1' 'k % 10 != 0'; do
    $CLICKHOUSE_LOCAL -q "
        SELECT *, tuple(*) = (
            SELECT tuple(count(), countIf(if(k < 1500, intDiv(k, 100) % 2 = 0, k % 3 = 0)))
            FROM (SELECT number AS k FROM numbers(3000)) WHERE $f)
        FROM (SELECT count(), countIf(flag) FROM file('$BOOL_FILE') PREWHERE $f)
        SETTINGS $SETTINGS"
done
