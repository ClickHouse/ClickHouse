-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: the second reads rely on the entries the first ones wrote to the server-wide query condition cache, which other tests clear

-- A `minmax` index on the `ORDER BY ... LIMIT` column skips the granules beyond the running top-K threshold before
-- PREWHERE. A PREWHERE that holds `__topKFilter` still records them in the query condition cache, so a repeated query
-- selects fewer marks. A sort type with a floating-point part keeps the cache unused: a NaN bound does not bound a granule.

-- CI randomizes these settings, and each of them can turn off the top-K skip, the PREWHERE or the cache.
SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;
SET use_skip_indexes_for_top_k = 1;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_lazy_materialization = 0;
SET enable_parallel_replicas = 0;
-- The stateless-test profile sets `max_rows_to_read`, and a throwing row limit turns off the skip indexes on data read.
SET max_rows_to_read = 0, max_rows_to_read_leaf = 0;
-- One stream reading one granule per block, so the threshold is set after the first granule.
SET max_threads = 1;
SET max_block_size = 1000;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS tab;

-- `d` descends along the primary key and the other sort columns ascend, so the first granule holds the top rows.
CREATE TABLE tab (id UInt64, a UInt64, d UInt64, f Float64, lf LowCardinality(Float64), lu LowCardinality(UInt64), s String,
    INDEX a_mm a TYPE minmax GRANULARITY 1, INDEX d_mm d TYPE minmax GRANULARITY 1, INDEX f_mm f TYPE minmax GRANULARITY 1,
    INDEX lf_mm lf TYPE minmax GRANULARITY 1, INDEX lu_mm lu TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1000, add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab SELECT number, number, 99999 - number, number, number, number, if(number % 3 = 0, '', toString(number)) FROM numbers(100000);

SYSTEM DROP QUERY CONDITION CACHE;

SELECT a FROM tab WHERE s != '' ORDER BY a LIMIT 5 SETTINGS log_comment = '05350_asc_1';
SELECT a FROM tab WHERE s != '' ORDER BY a LIMIT 5 SETTINGS log_comment = '05350_asc_2';
SELECT d FROM tab WHERE s != '' ORDER BY d DESC LIMIT 5 SETTINGS log_comment = '05350_desc_1';
SELECT d FROM tab WHERE s != '' ORDER BY d DESC LIMIT 5 SETTINGS log_comment = '05350_desc_2';
SELECT f FROM tab WHERE s != '' ORDER BY f LIMIT 5 SETTINGS log_comment = '05350_float_1';
SELECT f FROM tab WHERE s != '' ORDER BY f LIMIT 5 SETTINGS log_comment = '05350_float_2';
SELECT lf FROM tab WHERE s != '' ORDER BY lf LIMIT 5 SETTINGS log_comment = '05350_lcfloat_1';
SELECT lf FROM tab WHERE s != '' ORDER BY lf LIMIT 5 SETTINGS log_comment = '05350_lcfloat_2';
SELECT lu FROM tab WHERE s != '' ORDER BY lu LIMIT 5 SETTINGS log_comment = '05350_lcuint_1';
SELECT lu FROM tab WHERE s != '' ORDER BY lu LIMIT 5 SETTINGS log_comment = '05350_lcuint_2';

SYSTEM FLUSH LOGS query_log;

-- The first run skips granules by the `minmax` index, so it reads fewer rows than the table has. The repeated query
-- hits the query condition cache and selects fewer marks, except for a floating-point sort type.
SELECT
    splitByChar('_', log_comment)[2] AS arm,
    anyIf(ProfileEvents['QueryConditionCacheHits'], endsWith(log_comment, '_2')) > 0,
    anyIf(ProfileEvents['SelectedMarks'], endsWith(log_comment, '_2')) < anyIf(ProfileEvents['SelectedMarks'], endsWith(log_comment, '_1')),
    anyIf(read_rows, endsWith(log_comment, '_1')) < 100000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05350_asc_1', '05350_asc_2', '05350_desc_1', '05350_desc_2', '05350_float_1', '05350_float_2',
        '05350_lcfloat_1', '05350_lcfloat_2', '05350_lcuint_1', '05350_lcuint_2')
    AND event_date >= yesterday()
GROUP BY arm
ORDER BY arm;

-- Without `__topKFilter` in the PREWHERE the entry is not salted with the top-K plan, so the granules skipped by the
-- threshold must not be recorded: a later read with the same PREWHERE and no top-K still reads every row.
-- Reading `a` keeps `s != ''` in PREWHERE for that read.
SYSTEM DROP QUERY CONDITION CACHE;
SELECT a FROM tab WHERE s != '' ORDER BY a LIMIT 5 SETTINGS use_top_k_dynamic_filtering = 0;
SELECT count(), sum(a) FROM tab WHERE s != '';

DROP TABLE tab;
