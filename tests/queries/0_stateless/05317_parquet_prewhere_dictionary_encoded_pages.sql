-- Tags: no-fasttest
-- PREWHERE over columns stored in dictionary-encoded pages of a Parquet file without a page index
-- returns exactly the rows that a direct computation over the same data returns.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05317.parquet')
SELECT
    number AS k,
    toUInt8(number % 7) AS bp,
    toUInt16(intDiv(number, 100) % 5) AS rle,
    'v' || toString(intDiv(number, 30) % 11) AS s,
    toUInt64(42) AS c,
    (toFloat64(number % 3), toFloat64(number % 4))::Point AS p
FROM numbers(30000)
SETTINGS output_format_parquet_write_page_index = 0, output_format_parquet_row_group_size = 10000,
    output_format_parquet_data_page_size = 1024, output_format_parquet_batch_size = 100;

-- The file must keep these columns dictionary-encoded, otherwise the queries below test nothing.
SELECT tupleElement(arrayJoin(columns) AS col, 'name'), has(tupleElement(col, 'encodings'), 'RLE_DICTIONARY')
FROM file(currentDatabase() || '_05317.parquet', ParquetMetadata)
WHERE tupleElement(col, 'name') != 'k';

SELECT 'S', *, tuple(*) = (
    SELECT tuple(count(), sum(toUInt8(number % 7)), sum(toUInt16(intDiv(number, 100) % 5)), sum(toUInt64(42)),
        sum(cityHash64('v' || toString(intDiv(number, 30) % 11))),
        sum(cityHash64((toFloat64(number % 3), toFloat64(number % 4))::Point)),
        sum(cityHash64(number, toUInt8(number % 7), toUInt16(intDiv(number, 100) % 5),
            'v' || toString(intDiv(number, 30) % 11), toUInt64(42), (toFloat64(number % 3), toFloat64(number % 4))::Point)))
    FROM numbers(30000) WHERE number % 7 = 3)
FROM (SELECT count(), sum(bp), sum(rle), sum(c), sum(cityHash64(s)), sum(cityHash64(p)), sum(cityHash64(k, bp, rle, s, c, p))
    FROM file(currentDatabase() || '_05317.parquet') PREWHERE k % 7 = 3)
SETTINGS input_format_parquet_max_block_size = 1000, input_format_parquet_prefer_block_bytes = 0,
    input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_dictionary_filter_push_down = 0, use_query_condition_cache = 0;

SELECT 'D', *, tuple(*) = (
    SELECT tuple(count(), sum(toUInt8(number % 7)), sum(toUInt16(intDiv(number, 100) % 5)), sum(toUInt64(42)),
        sum(cityHash64('v' || toString(intDiv(number, 30) % 11))),
        sum(cityHash64((toFloat64(number % 3), toFloat64(number % 4))::Point)),
        sum(cityHash64(number, toUInt8(number % 7), toUInt16(intDiv(number, 100) % 5),
            'v' || toString(intDiv(number, 30) % 11), toUInt64(42), (toFloat64(number % 3), toFloat64(number % 4))::Point)))
    FROM numbers(30000) WHERE number % 10 != 0)
FROM (SELECT count(), sum(bp), sum(rle), sum(c), sum(cityHash64(s)), sum(cityHash64(p)), sum(cityHash64(k, bp, rle, s, c, p))
    FROM file(currentDatabase() || '_05317.parquet') PREWHERE k % 10 != 0)
SETTINGS input_format_parquet_max_block_size = 1000, input_format_parquet_prefer_block_bytes = 0,
    input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_dictionary_filter_push_down = 0, use_query_condition_cache = 0;

SELECT 'A', *, tuple(*) = (
    SELECT tuple(count(), sum(toUInt8(number % 7)), sum(toUInt16(intDiv(number, 100) % 5)), sum(toUInt64(42)),
        sum(cityHash64('v' || toString(intDiv(number, 30) % 11))),
        sum(cityHash64((toFloat64(number % 3), toFloat64(number % 4))::Point)),
        sum(cityHash64(number, toUInt8(number % 7), toUInt16(intDiv(number, 100) % 5),
            'v' || toString(intDiv(number, 30) % 11), toUInt64(42), (toFloat64(number % 3), toFloat64(number % 4))::Point)))
    FROM numbers(30000) WHERE number < 1000000)
FROM (SELECT count(), sum(bp), sum(rle), sum(c), sum(cityHash64(s)), sum(cityHash64(p)), sum(cityHash64(k, bp, rle, s, c, p))
    FROM file(currentDatabase() || '_05317.parquet') PREWHERE k < 1000000)
SETTINGS input_format_parquet_max_block_size = 1000, input_format_parquet_prefer_block_bytes = 0,
    input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_dictionary_filter_push_down = 0, use_query_condition_cache = 0;

SELECT 'P', *, tuple(*) = (
    SELECT tuple(count(), sum(toUInt8(number % 7)), sum(toUInt16(intDiv(number, 100) % 5)), sum(toUInt64(42)),
        sum(cityHash64('v' || toString(intDiv(number, 30) % 11))),
        sum(cityHash64((toFloat64(number % 3), toFloat64(number % 4))::Point)),
        sum(cityHash64(number, toUInt8(number % 7), toUInt16(intDiv(number, 100) % 5),
            'v' || toString(intDiv(number, 30) % 11), toUInt64(42), (toFloat64(number % 3), toFloat64(number % 4))::Point)))
    FROM numbers(30000) WHERE number % 997 = 1)
FROM (SELECT count(), sum(bp), sum(rle), sum(c), sum(cityHash64(s)), sum(cityHash64(p)), sum(cityHash64(k, bp, rle, s, c, p))
    FROM file(currentDatabase() || '_05317.parquet') PREWHERE k % 997 = 1)
SETTINGS input_format_parquet_max_block_size = 1000, input_format_parquet_prefer_block_bytes = 0,
    input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_dictionary_filter_push_down = 0, use_query_condition_cache = 0;

SELECT 'R', *, tuple(*) = (
    SELECT tuple(count(), sum(toUInt8(number % 7)), sum(toUInt16(intDiv(number, 100) % 5)), sum(toUInt64(42)),
        sum(cityHash64('v' || toString(intDiv(number, 30) % 11))),
        sum(cityHash64((toFloat64(number % 3), toFloat64(number % 4))::Point)),
        sum(cityHash64(number, toUInt8(number % 7), toUInt16(intDiv(number, 100) % 5),
            'v' || toString(intDiv(number, 30) % 11), toUInt64(42), (toFloat64(number % 3), toFloat64(number % 4))::Point)))
    FROM numbers(30000) WHERE intDiv(number, 37) % 3 = 0)
FROM (SELECT count(), sum(bp), sum(rle), sum(c), sum(cityHash64(s)), sum(cityHash64(p)), sum(cityHash64(k, bp, rle, s, c, p))
    FROM file(currentDatabase() || '_05317.parquet') PREWHERE intDiv(k, 37) % 3 = 0)
SETTINGS input_format_parquet_max_block_size = 1000, input_format_parquet_prefer_block_bytes = 0,
    input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_dictionary_filter_push_down = 0, use_query_condition_cache = 0;

SELECT 'N', *, tuple(*) = (
    SELECT tuple(count(), sum(toUInt8(number % 7)), sum(toUInt16(intDiv(number, 100) % 5)), sum(toUInt64(42)),
        sum(cityHash64('v' || toString(intDiv(number, 30) % 11))),
        sum(cityHash64((toFloat64(number % 3), toFloat64(number % 4))::Point)),
        sum(cityHash64(number, toUInt8(number % 7), toUInt16(intDiv(number, 100) % 5),
            'v' || toString(intDiv(number, 30) % 11), toUInt64(42), (toFloat64(number % 3), toFloat64(number % 4))::Point)))
    FROM numbers(30000) WHERE number BETWEEN 12345 AND 12400)
FROM (SELECT count(), sum(bp), sum(rle), sum(c), sum(cityHash64(s)), sum(cityHash64(p)), sum(cityHash64(k, bp, rle, s, c, p))
    FROM file(currentDatabase() || '_05317.parquet') PREWHERE k BETWEEN 12345 AND 12400)
SETTINGS input_format_parquet_max_block_size = 1000, input_format_parquet_prefer_block_bytes = 0,
    input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_dictionary_filter_push_down = 0, use_query_condition_cache = 0;

-- Lazily read rows, one in each 1000-row block of the last row group.
SELECT k, bp, rle, s, c, p FROM file(currentDatabase() || '_05317.parquet') ORDER BY k % 1000 DESC, k DESC LIMIT 10
SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 100,
    input_format_parquet_max_block_size = 1000, input_format_parquet_prefer_block_bytes = 0,
    input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_dictionary_filter_push_down = 0, use_query_condition_cache = 0;
