-- Tags: no-fasttest
-- no-fasttest: COLLATE needs ICU

-- The dynamic top-K threshold of ORDER BY ... LIMIT is collected across the blocks of all sorting streams.

SET use_query_condition_cache = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000;
SET use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;
-- The stateless-test profile sets max_rows_to_read, and a throwing row limit disables skip indexes on data read.
SET max_rows_to_read = 0, max_rows_to_read_leaf = 0;
SET allow_prefetched_read_pool_for_local_filesystem = 0, allow_prefetched_read_pool_for_remote_filesystem = 0;
SET max_bytes_before_remerge_sort = 1000000000, max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0;

DROP TABLE IF EXISTS t_single;
DROP TABLE IF EXISTS t_multi;
DROP TABLE IF EXISTS t_streams;

-- 149 rows in one part, k = id. Every block (5 rows) is shorter than LIMIT 100 and the input is shorter than
-- 1.5 * LIMIT, so neither a single block nor a remerge publishes a threshold.
CREATE TABLE t_single
(
    id UInt64,
    k UInt64,
    b UInt64,
    f Float64,
    INDEX k_minmax k TYPE minmax GRANULARITY 1,
    INDEX f_minmax f TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 5;

INSERT INTO t_single SELECT number, number, multiIf(number = 99, 200, number = 140, 99, number), if(number BETWEEN 140 AND 144, nan, number)
FROM numbers(149);

SELECT 'collected threshold skips marks';
SELECT k FROM t_single WHERE id != 1000 ORDER BY k LIMIT 100 FORMAT Null
SETTINGS max_threads = 1, max_block_size = 5, log_comment = '05339_engagement';

SYSTEM FLUSH LOGS query_log;
SELECT read_rows < 149 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05339_engagement' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- The first 100 rows hold b = 0..98 and 200, so the threshold is 200 and the later b = 99 must pass it.
SELECT 'boundary value';
SELECT max(b), count() FROM
(
    SELECT b FROM t_single WHERE id != 1000 ORDER BY b LIMIT 100
    SETTINGS max_threads = 1, max_block_size = 5
);

SELECT 'nan sorted first is kept';
SELECT countIf(isNaN(f)), count(), maxIf(f, NOT isNaN(f)) FROM
(
    SELECT f FROM t_single WHERE id != 1000 ORDER BY f NULLS FIRST LIMIT 100
    SETTINGS max_threads = 1, max_block_size = 5
);

-- 8 parts of 60 rows, one per stream: in each part the first 30 rows have small keys, the other 30 the key 1000000.
-- A threshold over fewer than 100 small keys is 1000000 and skips nothing, so only one collected across at least
-- 4 parts skips the large-key marks.
CREATE TABLE t_streams (id UInt64, k UInt64, INDEX k_minmax k TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 5;

SYSTEM STOP MERGES t_streams;
INSERT INTO t_streams SELECT number, if(number % 60 < 30, number, 1000000) FROM numbers(0, 60);
INSERT INTO t_streams SELECT number, if(number % 60 < 30, number, 1000000) FROM numbers(60, 60);
INSERT INTO t_streams SELECT number, if(number % 60 < 30, number, 1000000) FROM numbers(120, 60);
INSERT INTO t_streams SELECT number, if(number % 60 < 30, number, 1000000) FROM numbers(180, 60);
INSERT INTO t_streams SELECT number, if(number % 60 < 30, number, 1000000) FROM numbers(240, 60);
INSERT INTO t_streams SELECT number, if(number % 60 < 30, number, 1000000) FROM numbers(300, 60);
INSERT INTO t_streams SELECT number, if(number % 60 < 30, number, 1000000) FROM numbers(360, 60);
INSERT INTO t_streams SELECT number, if(number % 60 < 30, number, 1000000) FROM numbers(420, 60);

SELECT 'collected across streams skips marks';
SELECT k FROM t_streams WHERE id != 1000000 ORDER BY k LIMIT 100 FORMAT Null
SETTINGS max_threads = 8, max_block_size = 5, log_comment = '05339_streams';

SYSTEM FLUSH LOGS query_log;
SELECT read_rows < 480 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05339_streams' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT max(k), count() FROM
(
    SELECT k FROM t_streams WHERE id != 1000000 ORDER BY k LIMIT 100
    SETTINGS max_threads = 8, max_block_size = 5
);

-- 4 parts of 1000 rows: k has 50 values, 80 rows each; n is NULL for id % 7 = 0.
CREATE TABLE t_multi (id UInt64, k UInt64, n Nullable(UInt64), s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 5;

SYSTEM STOP MERGES t_multi;
INSERT INTO t_multi SELECT number, number % 50, if(number % 7 = 0, NULL, number % 50), concat(if(number % 2 = 0, 'a', 'B'), toString(number % 50)) FROM numbers(0, 1000);
INSERT INTO t_multi SELECT number, number % 50, if(number % 7 = 0, NULL, number % 50), concat(if(number % 2 = 0, 'a', 'B'), toString(number % 50)) FROM numbers(1000, 1000);
INSERT INTO t_multi SELECT number, number % 50, if(number % 7 = 0, NULL, number % 50), concat(if(number % 2 = 0, 'a', 'B'), toString(number % 50)) FROM numbers(2000, 1000);
INSERT INTO t_multi SELECT number, number % 50, if(number % 7 = 0, NULL, number % 50), concat(if(number % 2 = 0, 'a', 'B'), toString(number % 50)) FROM numbers(3000, 1000);

SET max_threads = 4, max_block_size = 5;

SELECT 'ties';
SELECT k, count() FROM (SELECT k FROM t_multi ORDER BY k LIMIT 200) GROUP BY k ORDER BY k;
SELECT 'desc offset';
SELECT k FROM t_multi ORDER BY k DESC LIMIT 5 OFFSET 158;
SELECT 'two columns';
SELECT id, k FROM t_multi ORDER BY k, id LIMIT 5 OFFSET 78;
SELECT 'nulls first';
SELECT n FROM t_multi ORDER BY n NULLS FIRST LIMIT 3 OFFSET 570;
SELECT count(), countIf(n IS NULL) FROM (SELECT n FROM t_multi ORDER BY n NULLS FIRST LIMIT 100);
SELECT 'nulls last';
SELECT n FROM t_multi ORDER BY n DESC NULLS LAST LIMIT 2 OFFSET 67;
SELECT n FROM t_multi ORDER BY n DESC NULLS FIRST LIMIT 3 OFFSET 571;
SELECT 'collation';
SELECT s FROM t_multi ORDER BY s COLLATE 'en' LIMIT 3 OFFSET 79 SETTINGS use_top_k_dynamic_filtering_for_variable_length_types = 1;
SELECT s FROM t_multi ORDER BY s DESC COLLATE 'en' LIMIT 2 OFFSET 79 SETTINGS use_top_k_dynamic_filtering_for_variable_length_types = 1;

DROP TABLE t_single;
DROP TABLE t_multi;
DROP TABLE t_streams;
