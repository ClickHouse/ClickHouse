-- Tags: no-parallel-replicas

-- `GROUP BY ... LIMIT n` keeps a top-K heap of the groups and publishes its boundary to the read of the first
-- ranked key column. A `minmax` index on that column skips the granules whose bound lies beyond the boundary while
-- reading, as the primary key does when the column is a key column. The result must equal `ORDER BY` the keys with
-- the top-K optimizations off.

-- CI randomizes these settings, and each of them can turn off the heap, its filter or the skip indexes on data read.
SET enable_group_by_top_k_optimization = 1;
SET enable_group_by_top_k_dynamic_filtering = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_for_top_k = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_parallel_replicas = 0;
-- The result must not depend on the server-wide condition cache.
SET use_query_condition_cache = 0;
-- The stateless-test profile sets `max_rows_to_read` and `max_rows_to_group_by`. A throwing row limit turns off the
-- skip indexes on data read, and a limit on the GROUP BY rows turns off the heap.
SET max_rows_to_read = 0, max_rows_to_read_leaf = 0;
SET max_rows_to_group_by = 0;
-- One stream reading one granule per block, so the boundary is set after the first granule.
SET max_threads = 1;
SET max_block_size = 128;

DROP TABLE IF EXISTS t_gbmm;
DROP TABLE IF EXISTS t_gbmm_auto;
DROP TABLE IF EXISTS t_gbmm_nan;

-- The primary key `v` says nothing about the ranked columns. `k` and `ki` are clustered per granule in shuffled order,
-- so granules can be skipped by their bounds, and the boundary keys span several granules.
CREATE TABLE t_gbmm (v UInt64, k UInt64, ki Int32, d Date, f Float64, s String,
    INDEX ik k TYPE minmax GRANULARITY 1, INDEX iki ki TYPE minmax GRANULARITY 1, INDEX id d TYPE minmax GRANULARITY 2,
    INDEX iff f TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY v
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_gbmm SELECT number,
    ((intDiv(number, 128) * 7919) % 1000) * 100 + number % 37,
    toInt32(((intDiv(number, 100) * 104729) % 2000) - 1000) * 10 + toInt32(number % 3),
    toDate('2020-01-01') + ((intDiv(number, 300) * 31) % 400),
    ((intDiv(number, 128) * 7919) % 1000) * 100 + number % 37,
    toString(number % 11)
FROM numbers(200000);
INSERT INTO t_gbmm SELECT 200000 + number, 5 + number % 2, -9999 + number % 2, toDate('2019-06-01'), 5 + number % 2, 'x' FROM numbers(1000);

SELECT 'asc k', arraySort(groupArray((k, c))) FROM (SELECT k, count() c FROM t_gbmm GROUP BY k LIMIT 7) SETTINGS log_comment = '05233_mm_k';
SELECT 'ref', arraySort(groupArray((k, c))) FROM (SELECT k, count() c FROM t_gbmm GROUP BY k ORDER BY k LIMIT 7 SETTINGS enable_group_by_top_k_optimization = 0, use_skip_indexes = 0);
SELECT 'desc k', arraySort(groupArray((k, c, sv))) FROM (SELECT k, count() c, sum(v) sv FROM t_gbmm GROUP BY k ORDER BY k DESC LIMIT 5) SETTINGS log_comment = '05233_mm_kdesc';
SELECT 'ref', arraySort(groupArray((k, c, sv))) FROM (SELECT k, count() c, sum(v) sv FROM t_gbmm GROUP BY k ORDER BY k DESC LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 0, use_skip_indexes = 0);
SELECT 'asc ki, s', arraySort(groupArray((ki, s, c))) FROM (SELECT ki, s, count() c FROM t_gbmm GROUP BY ki, s LIMIT 9) SETTINGS log_comment = '05233_mm_kis';
SELECT 'ref', arraySort(groupArray((ki, s, c))) FROM (SELECT ki, s, count() c FROM t_gbmm GROUP BY ki, s ORDER BY ki, s LIMIT 9 SETTINGS enable_group_by_top_k_optimization = 0, use_skip_indexes = 0);
SELECT 'desc ki', arraySort(groupArray((ki, c))) FROM (SELECT ki, count() c FROM t_gbmm GROUP BY ki ORDER BY ki DESC LIMIT 4);
SELECT 'ref', arraySort(groupArray((ki, c))) FROM (SELECT ki, count() c FROM t_gbmm GROUP BY ki ORDER BY ki DESC LIMIT 4 SETTINGS enable_group_by_top_k_optimization = 0, use_skip_indexes = 0);
SELECT 'asc d, granularity 2', arraySort(groupArray((d, c))) FROM (SELECT d, count() c FROM t_gbmm GROUP BY d ORDER BY d LIMIT 3);
SELECT 'ref', arraySort(groupArray((d, c))) FROM (SELECT d, count() c FROM t_gbmm GROUP BY d ORDER BY d LIMIT 3 SETTINGS enable_group_by_top_k_optimization = 0, use_skip_indexes = 0);
SELECT 'where, asc k', arraySort(groupArray((k, c))) FROM (SELECT k, count() c FROM t_gbmm WHERE s != '3' GROUP BY k LIMIT 6);
SELECT 'ref', arraySort(groupArray((k, c))) FROM (SELECT k, count() c FROM t_gbmm WHERE s != '3' GROUP BY k ORDER BY k LIMIT 6 SETTINGS enable_group_by_top_k_optimization = 0, use_skip_indexes = 0);
SELECT 'offset', arraySort(groupArray((k, c))) FROM (SELECT k, count() c FROM t_gbmm GROUP BY k ORDER BY k LIMIT 3 OFFSET 4);
SELECT 'ref', arraySort(groupArray((k, c))) FROM (SELECT k, count() c FROM t_gbmm GROUP BY k ORDER BY k LIMIT 3 OFFSET 4 SETTINGS enable_group_by_top_k_optimization = 0, use_skip_indexes = 0);
SELECT 'four threads', arraySort(groupArray((k, c))) FROM (SELECT k, count() c FROM t_gbmm GROUP BY k LIMIT 7 SETTINGS max_threads = 4, max_block_size = 65409);
-- Floating-point columns are left out: a NaN bound does not bound a granule.
SELECT 'float', arraySort(groupArray((f, c))) FROM (SELECT f, count() c FROM t_gbmm GROUP BY f LIMIT 7) SETTINGS log_comment = '05233_mm_float';
SELECT 'ignored index', arraySort(groupArray((k, c))) FROM (SELECT k, count() c FROM t_gbmm GROUP BY k LIMIT 7) SETTINGS ignore_data_skipping_indices = 'ik', log_comment = '05233_mm_ignored';

-- An implicit `minmax` index (`add_minmax_index_for_numeric_columns`) is used the same way.
CREATE TABLE t_gbmm_auto (v UInt64, k UInt64) ENGINE = MergeTree ORDER BY v
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_gbmm_auto SELECT v, k FROM t_gbmm;
SELECT 'implicit index', arraySort(groupArray((k, c))) FROM (SELECT k, count() c FROM t_gbmm_auto GROUP BY k LIMIT 7) SETTINGS log_comment = '05233_mm_auto';

-- The last index granule covers an all-NaN granule and the granule with the largest finite values, so its stored max is
-- NaN. A descending boundary must not skip it.
CREATE TABLE t_gbmm_nan (v UInt64, f Float64, INDEX iff f TYPE minmax GRANULARITY 2) ENGINE = MergeTree ORDER BY v
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_gbmm_nan SELECT number, multiIf(number < 1280, number % 100, number < 1408, nan, 1000 + number % 3) FROM numbers(1536);
SELECT 'float desc, nan max', arraySort(groupArray((f, c))) FROM (SELECT f, count() c FROM t_gbmm_nan GROUP BY f ORDER BY f DESC LIMIT 3);
SELECT 'ref', arraySort(groupArray((f, c))) FROM (SELECT f, count() c FROM t_gbmm_nan GROUP BY f ORDER BY f DESC LIMIT 3 SETTINGS enable_group_by_top_k_optimization = 0, use_skip_indexes = 0);

SYSTEM FLUSH LOGS query_log;

-- With the index, the read stops at the granules that can hold a group of the result; without it, every row is read.
SELECT log_comment, read_rows < 201000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment IN ('05233_mm_k', '05233_mm_kdesc', '05233_mm_kis', '05233_mm_float', '05233_mm_ignored', '05233_mm_auto')
ORDER BY log_comment
SETTINGS max_block_size = 65409;

DROP TABLE t_gbmm;
DROP TABLE t_gbmm_auto;
DROP TABLE t_gbmm_nan;
