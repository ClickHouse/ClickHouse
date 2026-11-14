-- Tags: no-fasttest
-- Tag no-fasttest: Parquet, MinIO for url()

-- A file() / url() read with a PREWHERE does not return the columns that only the PREWHERE uses.

SET engine_file_truncate_on_insert = 1, s3_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05327.parquet', Parquet,
    'id UInt64, s String, u String, w UInt32, n Nullable(String), lc LowCardinality(String), arr Array(UInt32), t Tuple(a UInt32, b String), f UInt8')
SELECT number, if(number % 7 = 0, 'phrase' || toString(number), ''), 'http://x/' || toString(number), number % 1000,
    if(number % 3 = 0, NULL, toString(number % 5)), toString(number % 11), [number % 13, number % 17], (number % 19, toString(number)), number % 2
FROM numbers(100000)
SETTINGS output_format_parquet_row_group_size = 10000;

INSERT INTO FUNCTION file(currentDatabase() || '_05327_glob_1.parquet', Parquet, 'id UInt64, s String, w UInt32')
SELECT number, if(number % 7 = 0, 'x', ''), number % 1000 FROM numbers(5000);
INSERT INTO FUNCTION file(currentDatabase() || '_05327_glob_2.parquet', Parquet, 'id UInt64, s String, w UInt32')
SELECT number, if(number % 5 = 0, 'x', ''), number % 1000 FROM numbers(5000);

INSERT INTO FUNCTION file(currentDatabase() || '_05327_hive/key=1/data.parquet', Parquet, 'id UInt64, s String, w UInt32')
SELECT number, if(number % 7 = 0, 'x', ''), number % 1000 FROM numbers(1000);
INSERT INTO FUNCTION file(currentDatabase() || '_05327_hive/key=2/data.parquet', Parquet, 'id UInt64, s String, w UInt32')
SELECT number, if(number % 5 = 0, 'x', ''), number % 1000 FROM numbers(1000);

INSERT INTO FUNCTION s3(s3_conn, filename = '05327/' || currentDatabase() || '.parquet', format = Parquet, structure = 'id UInt64, s String, w UInt32')
SELECT number, if(number % 7 = 0, 'x', ''), number % 1000 FROM numbers(10000);

-- Each EXPLAIN returns (PREWHERE present, PREWHERE-only column in the reading step's header).
SELECT 'moved WHERE', countIf(explain ILIKE '%Prewhere filter column%'), countIf(explain ILIKE '% s String')
FROM (EXPLAIN header = 1 SELECT id % 10 AS k, count(), sum(w) FROM file(currentDatabase() || '_05327.parquet') WHERE s != '' GROUP BY k
    SETTINGS optimize_move_to_prewhere = 1, query_plan_remove_unused_columns = 1);

SELECT 'explicit PREWHERE', countIf(explain ILIKE '%Prewhere filter column%'), countIf(explain ILIKE '% u String')
FROM (EXPLAIN header = 1 SELECT count() FROM file(currentDatabase() || '_05327.parquet') PREWHERE u != ''
    SETTINGS optimize_move_to_prewhere = 1, query_plan_remove_unused_columns = 1);

SELECT 'url', countIf(explain ILIKE '%Prewhere filter column%'), countIf(explain ILIKE '% s String')
FROM (EXPLAIN header = 1 SELECT id % 10 AS k, count(), sum(w)
    FROM url('http://localhost:11111/test/05327/' || currentDatabase() || '.parquet', Parquet) WHERE s != '' GROUP BY k
    SETTINGS optimize_move_to_prewhere = 1, query_plan_remove_unused_columns = 1, enable_parallel_replicas = 0);

SELECT 'pruning key', countIf(explain ILIKE '%Prewhere filter column%'), countIf(explain ILIKE '% id UInt64')
FROM (EXPLAIN header = 1 SELECT sum(w) FROM file(currentDatabase() || '_05327.parquet') WHERE id < 5000
    SETTINGS optimize_move_to_prewhere = 1, query_plan_remove_unused_columns = 1);

SELECT 'bare PREWHERE', countIf(explain ILIKE '%Prewhere filter column%'), countIf(explain ILIKE '% f UInt8')
FROM (EXPLAIN header = 1 SELECT count() FROM file(currentDatabase() || '_05327.parquet') PREWHERE f
    SETTINGS optimize_move_to_prewhere = 1, query_plan_remove_unused_columns = 1);

SELECT 'bare WHERE', countIf(explain ILIKE '%Prewhere filter column%'), countIf(explain ILIKE '% f UInt8')
FROM (EXPLAIN header = 1 SELECT sum(w) FROM file(currentDatabase() || '_05327.parquet') WHERE f
    SETTINGS optimize_move_to_prewhere = 1, query_plan_remove_unused_columns = 1);

-- A column with a DEFAULT expression keeps every PREWHERE input in the header.
CREATE TABLE t_def (id UInt64, s String, w UInt32, d UInt32 DEFAULT w + 1) ENGINE = File(Parquet);
INSERT INTO t_def SELECT number, if(number % 7 = 0, 'x', ''), number % 1000, number % 1000 + 1 FROM numbers(10000);
SELECT 'default', countIf(explain ILIKE '%Prewhere filter column%'), countIf(explain ILIKE '% s String')
FROM (EXPLAIN header = 1 SELECT sum(d) FROM t_def WHERE s != '' SETTINGS optimize_move_to_prewhere = 1, query_plan_remove_unused_columns = 1);

SELECT 'results';
SELECT id % 10 AS k, count(), sum(w) FROM file(currentDatabase() || '_05327.parquet') WHERE s != '' GROUP BY k ORDER BY k;
SELECT count() FROM file(currentDatabase() || '_05327.parquet') PREWHERE u != '';
SELECT count() FROM file(currentDatabase() || '_05327.parquet') PREWHERE u != '' SETTINGS max_block_size = 1000;
SELECT count() FROM file(currentDatabase() || '_05327.parquet') PREWHERE u = 'none';
SELECT count(), sum(w) FROM file(currentDatabase() || '_05327_glob_{1,2}.parquet') WHERE s != '';
SELECT count(), sum(length(s)) FROM file(currentDatabase() || '_05327.parquet') WHERE s != '';
SELECT count(), sum(w) FROM file(currentDatabase() || '_05327.parquet') WHERE n = '2';
SELECT count(), sum(w) FROM file(currentDatabase() || '_05327.parquet') WHERE n IS NULL;
SELECT count(), sum(w) FROM file(currentDatabase() || '_05327.parquet') WHERE lc = '3';
SELECT count(), sum(w) FROM file(currentDatabase() || '_05327.parquet') WHERE has(arr, 7);
SELECT count(), sum(length(t.b)) FROM file(currentDatabase() || '_05327.parquet') WHERE t.a = 7;
SELECT sum(d), count() FROM t_def WHERE s != '';
SELECT _file LIKE '%05327%' AS f, count() FROM file(currentDatabase() || '_05327.parquet') WHERE s != '' GROUP BY f;
SELECT key, count(), sum(w) FROM file(currentDatabase() || '_05327_hive/key=*/data.parquet') WHERE s != '' GROUP BY key ORDER BY key
SETTINGS use_hive_partitioning = 1;
SELECT count(), sum(w) FROM url('http://localhost:11111/test/05327/' || currentDatabase() || '.parquet', Parquet) WHERE s != '';
SELECT count() FROM file(currentDatabase() || '_05327.parquet') PREWHERE f;
SELECT count(), sum(w) FROM file(currentDatabase() || '_05327.parquet') WHERE f;
SELECT f, count() FROM file(currentDatabase() || '_05327.parquet') WHERE f GROUP BY f;

-- Row group pruning still sees a PREWHERE-only column.
SELECT sum(w) FROM file(currentDatabase() || '_05327.parquet') WHERE id < 5000
SETTINGS input_format_parquet_filter_push_down = 1, use_query_condition_cache = 0, optimize_move_to_prewhere = 1,
    query_plan_remove_unused_columns = 1, log_comment = '05327_prune';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParquetPrunedRowGroups'] > 0 FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '05327_prune';

DROP TABLE t_def;
