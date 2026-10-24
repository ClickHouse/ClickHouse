-- Tags: no-fasttest
-- In the last PREWHERE step over a single dictionary-encoded column of a Parquet file, the functions applied
-- to that column run once per dictionary value. Results must match a direct computation, and the step must
-- not see a value of a row that an earlier step rejected.

SET engine_file_truncate_on_insert = 1;
SET input_format_parquet_max_block_size = 1000, input_format_parquet_prefer_block_bytes = 0,
    input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_dictionary_filter_push_down = 0, use_query_condition_cache = 0, use_columns_cache = 0,
    enable_multiple_prewhere_read_steps = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
    log_queries = 1;

DROP TABLE IF EXISTS ref;
-- Rows with k % 14 = 7 (odd k) hold values that throw: `x\` as a LIKE pattern, 'garbage' as a date.
CREATE TABLE ref ENGINE = Memory AS
SELECT
    number AS k,
    if(number % 14 = 7, 'x\\', ['', 'a', 'ab', 'b\0c', 'Google'][number % 5 + 1]) AS s,
    if(number % 14 = 7, 'garbage', toString(toDate('2020-01-01') + number % 10)) AS d,
    toFixedString(s, 16) AS f,
    toUInt32(number % 5) AS n,
    toNullable(s) AS ns,
    [s] AS a,
    toString(number % 5000) AS h
FROM numbers(30000);

-- Row groups of exactly 10000 rows, written in order.
INSERT INTO FUNCTION file(currentDatabase() || '_05331_a.parquet') SELECT * FROM ref ORDER BY k
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1,
    output_format_parquet_row_group_size = 10000, output_format_parquet_data_page_size = 1024,
    output_format_parquet_write_page_index = 0, output_format_parquet_batch_size = 100, max_bytes_before_external_sort = 0;

SELECT arrayMap(x -> tupleElement(x, 'num_rows'), row_groups) FROM file(currentDatabase() || '_05331_a.parquet', ParquetMetadata);
SELECT tupleElement(arrayJoin(columns) AS col, 'name'), has(tupleElement(col, 'encodings'), 'RLE_DICTIONARY')
FROM file(currentDatabase() || '_05331_a.parquet', ParquetMetadata);
SELECT 'ns null_count', arrayAll(rg -> arrayExists(c -> tupleElement(c, 'name') = 'ns'
    AND tupleElement(tupleElement(c, 'statistics'), 'null_count') = 0, tupleElement(rg, 'columns')), row_groups)
FROM file(currentDatabase() || '_05331_a.parquet', ParquetMetadata);

SELECT 'Q1', count(), sum(cityHash64(k, s, d)) = (SELECT sum(cityHash64(k, s, d)) FROM ref WHERE s != '')
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE s != '';
SELECT 'Q2', count(), sum(cityHash64(k, s, d)) = (SELECT sum(cityHash64(k, s, d)) FROM ref WHERE s IN ('a', 'b\0c') OR s = 'Google')
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE s IN ('a', 'b\0c') OR s = 'Google';
SELECT 'Q3', count(), sum(cityHash64(k, s, d)) = (SELECT sum(cityHash64(k, s, d)) FROM ref WHERE NOT empty(s) AND s < 'b')
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE NOT empty(s) AND s < 'b';
SELECT 'Q4', count(), sum(cityHash64(k, s, d)) = (SELECT sum(cityHash64(k, s, d)) FROM ref WHERE s LIKE '%o%')
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE s LIKE '%o%';
SELECT 'Q6', count(), sum(cityHash64(k, s, d)) = (SELECT sum(cityHash64(k, s, d)) FROM ref WHERE k % 3 = 0 AND s != 'a')
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE k % 3 = 0 AND s != 'a' SETTINGS log_comment = '05331:q6';
SELECT 'Q7', count() = (SELECT count() FROM ref WHERE s != '')
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE s != '' SETTINGS log_comment = '05331:q7';
SELECT 'Q8', count(), sum(cityHash64(k, s, d)) = (SELECT sum(cityHash64(k, s, d)) FROM ref WHERE s != 'a' AND (k % 3 = 0 OR s = 'ab'))
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE s != 'a' AND (k % 3 = 0 OR s = 'ab');
SELECT 'Q9', count(), sum(cityHash64(upper(s))) = (SELECT sum(cityHash64(upper(s))) FROM ref WHERE k % 3 = 0 AND upper(s) != 'A')
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE k % 3 = 0 AND upper(s) != 'A';
SELECT 'F', count(), sum(cityHash64(k, f)) = (SELECT sum(cityHash64(k, f)) FROM ref WHERE f != toFixedString('a', 16))
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE f != toFixedString('a', 16) SETTINGS log_comment = '05331:f';
-- Not read as dictionary indexes: values of at most 8 bytes, Nullable, Array, a dictionary larger than half the rows.
SELECT 'N', count(), sum(cityHash64(k, n)) = (SELECT sum(cityHash64(k, n)) FROM ref WHERE n != 1)
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE n != 1 SETTINGS log_comment = '05331:n';
SELECT 'NS', count(), sum(cityHash64(k, ns)) = (SELECT sum(cityHash64(k, ns)) FROM ref WHERE ns != '')
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE ns != '' SETTINGS log_comment = '05331:ns';
SELECT 'A', count(), sum(cityHash64(k, a)) = (SELECT sum(cityHash64(k, a)) FROM ref WHERE has(a, 'a'))
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE has(a, 'a') SETTINGS log_comment = '05331:a';
SELECT 'H', count(), sum(cityHash64(k, h)) = (SELECT sum(cityHash64(k, h)) FROM ref WHERE h != '1')
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE h != '1' SETTINGS log_comment = '05331:h';

-- Throwing values are only in rows that the first step rejects. The references do not evaluate the
-- throwing expressions: d = '2020-01-01' + k % 10 outside the rows with k % 14 = 7.
SELECT 'S1', count(), sum(cityHash64(k, s, d)) = (SELECT sum(cityHash64(k, s, d)) FROM ref WHERE k % 2 = 0 AND k % 10 < 4)
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE k % 2 = 0 AND toDate(d) < toDate('2020-01-05')
SETTINGS log_comment = '05331:s1';
SELECT 'S3', count(), sum(cityHash64(k, s, d)) = (SELECT sum(cityHash64(k, s, d)) FROM ref WHERE k % 2 = 0)
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE k % 2 = 0 AND s LIKE s SETTINGS log_comment = '05331:s3';
SELECT 'S5', count(), sum(cityHash64(k, s, d)) = (SELECT sum(cityHash64(k, s, d)) FROM ref WHERE k % 2 = 0 AND k % 10 >= 2)
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE k % 2 = 0 AND addDays(d, 1) > toDateTime('2020-01-03 00:00:00')
SETTINGS log_comment = '05331:s5';
-- A stateful function keeps the per-row evaluation.
SELECT 'S4', count(), sum(cityHash64(k, s, d)) = (SELECT sum(cityHash64(k, s, d)) FROM ref WHERE k % 3 = 0)
FROM file(currentDatabase() || '_05331_a.parquet') PREWHERE k % 3 = 0 AND timeSeriesTagsToGroup([('t', s)]) != 1000000
SETTINGS log_comment = '05331:s4';
-- d is first read by the first step, which rejects the throwing values, so the last step must not see them.
SELECT 'S6', count(), sum(cityHash64(k, s, d)) = (SELECT sum(cityHash64(k, s, d)) FROM ref WHERE k % 14 != 7 AND k % 3 = 0 AND k % 10 >= 2)
FROM file(currentDatabase() || '_05331_a.parquet')
PREWHERE d != 'garbage' AND k % 3 = 0 AND addDays(d, 1) > toDateTime('2020-01-03 00:00:00') SETTINGS log_comment = '05331:s6';

-- Row group 0 is dictionary-encoded, row groups 1 and 2 are PLAIN (their dictionary is too big).
INSERT INTO FUNCTION file(currentDatabase() || '_05331_b.parquet')
SELECT number AS k, if(number < 10000, 'v' || toString(number % 20), 'u' || leftPad(toString(number), 19, '0')) AS s
FROM numbers(30000)
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1,
    output_format_parquet_row_group_size = 10000, output_format_parquet_max_dictionary_size = 4096,
    output_format_parquet_data_page_size = 1024, output_format_parquet_write_page_index = 0, output_format_parquet_batch_size = 100;

SELECT arrayMap(x -> tupleElement(x, 'num_rows'), row_groups) FROM file(currentDatabase() || '_05331_b.parquet', ParquetMetadata);

SELECT 'B', count(), sum(cityHash64(k, s)) = (
    SELECT sum(cityHash64(number, if(number < 10000, 'v' || toString(number % 20), 'u' || leftPad(toString(number), 19, '0'))))
    FROM numbers(30000) WHERE if(number < 10000, 'v' || toString(number % 20), 'u' || leftPad(toString(number), 19, '0')) != 'v1')
FROM file(currentDatabase() || '_05331_b.parquet') PREWHERE s != 'v1' SETTINGS log_comment = '05331:b';
SELECT 'BL', count() FROM file(currentDatabase() || '_05331_b.parquet') PREWHERE s LIKE '%v1%'
SETTINGS apply_string_filters_during_scan = 1, log_comment = '05331:bl';

-- Page index and small pages: the column is read range by range.
INSERT INTO FUNCTION file(currentDatabase() || '_05331_c.parquet') SELECT * FROM ref ORDER BY k
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1,
    output_format_parquet_row_group_size = 10000, output_format_parquet_data_page_size = 1024,
    output_format_parquet_write_page_index = 1, output_format_parquet_batch_size = 100, max_bytes_before_external_sort = 0;

SELECT 'C', count(), sum(cityHash64(k, s, d)) = (SELECT sum(cityHash64(k, s, d)) FROM ref WHERE k BETWEEN 15000 AND 15100 AND s != '')
FROM file(currentDatabase() || '_05331_c.parquet') PREWHERE k BETWEEN 15000 AND 15100 AND s != ''
SETTINGS input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1, log_comment = '05331:c';

-- Rows read as dictionary indexes: all rows passing the earlier steps for the eligible queries, none otherwise.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ParquetRowsDecodedAsDictionaryIndexes']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05331:%' AND event_date >= yesterday()
ORDER BY log_comment;
-- Only the 20000 values of the PLAIN row groups are checked against the string filter.
SELECT 'BL checked', ProfileEvents['StringValueFilterValuesChecked']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05331:bl' AND event_date >= yesterday();

DROP TABLE ref;
