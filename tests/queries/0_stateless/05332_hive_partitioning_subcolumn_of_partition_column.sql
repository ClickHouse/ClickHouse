-- Tags: no-fasttest
-- Tag no-fasttest: depends on S3 and Parquet

-- A subcolumn of a Hive partition column comes from the path like the column itself.

SET engine_file_truncate_on_insert = 1, s3_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '/05332/key=9/data.parquet', Parquet, 'key Nullable(Int64), v Int64') SELECT NULL, number FROM numbers(1000) SETTINGS output_format_parquet_row_group_size = 100;
INSERT INTO FUNCTION file(currentDatabase() || '/05332/plain/key=9/data.parquet', Parquet, 'v Int64') SELECT number FROM numbers(10);
INSERT INTO FUNCTION file(currentDatabase() || '/05332/key=abc/data.jsonl', JSONEachRow, 's String, key Nullable(String)') VALUES ('x', NULL), ('y', 'zz');
INSERT INTO FUNCTION file(currentDatabase() || '/05332/only/a=1/key=9/data.parquet', Parquet, 'key Nullable(Int64)') SELECT NULL FROM numbers(1000) SETTINGS output_format_parquet_row_group_size = 100;
INSERT INTO FUNCTION file(currentDatabase() || '/05332/t=(1,2)/data.parquet', Parquet, 't Tuple(a Int64, b Int64), v Int64') SELECT (5, 5), number FROM numbers(10);
INSERT INTO FUNCTION file(currentDatabase() || '/05332/t=(1,2)/data.jsonl', JSONEachRow, 't Tuple(a Int64, b Int64), v Int64') SELECT (5, 5), number FROM numbers(10);
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05332/key=9/data.parquet', format = Parquet, structure = 'key Nullable(Int64), v Int64') SELECT NULL, number FROM numbers(1000) SETTINGS output_format_parquet_row_group_size = 100;
INSERT INTO FUNCTION file(currentDatabase() || '/05332/cache/key=9/data.parquet', Parquet, 'key Nullable(Int64), v Int64') SELECT NULL, number FROM numbers(1000);
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05332/cache/key=9/data.parquet', format = Parquet, structure = 'key Nullable(Int64), v Int64') SELECT NULL, number FROM numbers(1000);
INSERT INTO FUNCTION file(currentDatabase() || '/05332/topk/data.parquet', Parquet, 'v Int64') SELECT number FROM numbers(1000) SETTINGS output_format_parquet_row_group_size = 100;

CREATE TABLE t_05332_hive (v Int64, key String) ENGINE = S3(s3_conn, filename = currentDatabase() || '/05332/strategy', format = Parquet, partition_strategy = 'hive') PARTITION BY key;
INSERT INTO t_05332_hive VALUES (1, 'abc');

SET use_hive_partitioning = 1;

CREATE TABLE t_05332_file AS file(currentDatabase() || '/05332/key=9/data.parquet', Parquet, 'key Nullable(Int64), v Int64');
CREATE TABLE t_05332_cache AS file(currentDatabase() || '/05332/cache/key=9/data.parquet', Parquet, 'key Nullable(Int64), v Int64');
SELECT sleep(1) FORMAT Null;

-- { echoOn }

SELECT key, key.null, count() FROM file(currentDatabase() || '/05332/key=9/data.parquet', Parquet, 'key Nullable(Int64), v Int64') GROUP BY ALL;
SELECT v, key.null FROM file(currentDatabase() || '/05332/key=9/data.parquet', Parquet, 'key Nullable(Int64), v Int64') ORDER BY v LIMIT 2;
SELECT count() FROM file(currentDatabase() || '/05332/key=abc/data.jsonl', JSONEachRow, 's String, key Nullable(String)') WHERE key.null = 0;
SELECT count() FROM file(currentDatabase() || '/05332/key=abc/data.jsonl', JSONEachRow, 's String, key Nullable(String)') WHERE key.null = 0 AND _file = 'data.jsonl';
SELECT key.null, count() FROM file(currentDatabase() || '/05332/plain/key=9/data.parquet', Parquet, 'v Int64, key Nullable(Int64)') GROUP BY ALL;
SELECT s, key, key.null, key.size FROM file(currentDatabase() || '/05332/key=abc/data.jsonl', JSONEachRow, 's String, key Nullable(String)') ORDER BY s;
SELECT key.null, key.size FROM file(currentDatabase() || '/05332/key=abc/data.jsonl', JSONEachRow, 's String, key Nullable(String)') ORDER BY ALL;
SELECT key.null, count() FROM file(currentDatabase() || '/05332/only/a=1/key=9/data.parquet', Parquet, 'key Nullable(Int64)') GROUP BY ALL;
SELECT t, t.a, v FROM file(currentDatabase() || '/05332/t=(1,2)/data.parquet', Parquet, 't Tuple(a Int64, b Int64), v Int64') ORDER BY v LIMIT 1;
SELECT t.a, v FROM file(currentDatabase() || '/05332/t=(1,2)/data.jsonl', JSONEachRow, 't Tuple(a Int64, b Int64), v Int64') ORDER BY v LIMIT 1;
SELECT key, key.null, count() FROM s3(s3_conn, filename = currentDatabase() || '/05332/key=9/data.parquet', format = Parquet, structure = 'key Nullable(Int64), v Int64') GROUP BY ALL;
SELECT key, key.null, count() FROM url('http://localhost:11111/test/' || currentDatabase() || '/05332/key=9/data.parquet', Parquet, 'key Nullable(Int64), v Int64') GROUP BY ALL;
SELECT v, key.null FROM file(currentDatabase() || '/05332/key=9/data.parquet', Parquet, 'key Nullable(Int64), v Int64') ORDER BY v DESC LIMIT 2 SETTINGS query_plan_optimize_lazy_materialization = 1;
SELECT v, key, key.size FROM t_05332_hive;

-- The format must not filter on the file's values of a partition subcolumn.
SELECT count() FROM file(currentDatabase() || '/05332/t=(1,2)/data.parquet', Parquet, 't Tuple(a Int64, b Int64), v Int64') WHERE t.a = 1 AND v >= 0 SETTINGS input_format_parquet_filter_push_down = 1;
SELECT count() FROM file(currentDatabase() || '/05332/key=9/data.parquet', Parquet, 'key Nullable(Int64), v Int64') WHERE key.null = 0 SETTINGS input_format_parquet_filter_push_down = 1;

-- PREWHERE on a file column still reads its inputs when the query requests a partition subcolumn.
SELECT count() FROM file(currentDatabase() || '/05332/t=(1,2)/data.parquet', Parquet, 't Tuple(a Int64, b Int64), v Int64') PREWHERE v >= 0 WHERE t.a = 1;
SELECT count() FROM file(currentDatabase() || '/05332/key=9/data.parquet', Parquet, 'key Nullable(Int64), v Int64') PREWHERE v >= 0 WHERE key.null = 0;
SELECT count(), any(t.a) FROM file(currentDatabase() || '/05332/t=(1,2)/data.parquet', Parquet, 't Tuple(a Int64, b Int64), v Int64') PREWHERE v >= 0;
SELECT count(), any(key.null) FROM s3(s3_conn, filename = currentDatabase() || '/05332/key=9/data.parquet', format = Parquet, structure = 'key Nullable(Int64), v Int64') PREWHERE v >= 500 SETTINGS parallel_replicas_for_cluster_engines = 0;

CREATE ROW POLICY p_05332 ON t_05332_file USING key.null = 0 TO ALL;
SELECT count() FROM t_05332_file SETTINGS optimize_count_from_files = 0;
DROP ROW POLICY p_05332 ON t_05332_file;
CREATE ROW POLICY p_05332 ON t_05332_file USING v >= 500 TO ALL;
SELECT key.null, count() FROM t_05332_file PREWHERE v < 600 GROUP BY ALL;
DROP ROW POLICY p_05332 ON t_05332_file;

-- A filtered read must not store its row count as the file's count.
CREATE ROW POLICY p_05332_cache ON t_05332_cache USING v >= 500 TO ALL;
SELECT key.null, count() FROM t_05332_cache GROUP BY ALL SETTINGS use_cache_for_count_from_files = 1;
DROP ROW POLICY p_05332_cache ON t_05332_cache;
SELECT count() FROM t_05332_cache SETTINGS use_cache_for_count_from_files = 1, optimize_count_from_files = 1;
SELECT key.null, count() FROM s3(s3_conn, filename = currentDatabase() || '/05332/cache/key=9/data.parquet', format = Parquet, structure = 'key Nullable(Int64), v Int64') PREWHERE v >= 500 GROUP BY ALL SETTINGS use_cache_for_count_from_files = 1, parallel_replicas_for_cluster_engines = 0;
SELECT count() FROM s3(s3_conn, filename = currentDatabase() || '/05332/cache/key=9/data.parquet', format = Parquet, structure = 'key Nullable(Int64), v Int64') SETTINGS use_cache_for_count_from_files = 1, optimize_count_from_files = 1;
-- Neither does a read that the top-K filter cut short.
SELECT v FROM file(currentDatabase() || '/05332/topk/data.parquet', Parquet, 'v Int64') ORDER BY v LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, input_format_parquet_filter_push_down = 1, max_threads = 1, max_parsing_threads = 1, use_cache_for_count_from_files = 1;
SELECT count() FROM file(currentDatabase() || '/05332/topk/data.parquet', Parquet, 'v Int64') SETTINGS use_cache_for_count_from_files = 1, optimize_count_from_files = 1, optimize_trivial_count_query = 1;

-- { echoOff }

DROP TABLE t_05332_file;
DROP TABLE t_05332_cache;
DROP TABLE t_05332_hive;
