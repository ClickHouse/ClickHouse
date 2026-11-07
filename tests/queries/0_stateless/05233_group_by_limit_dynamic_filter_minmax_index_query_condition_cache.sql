-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: the repeated queries need the entries the first ones wrote to the server-wide query condition cache, which other tests clear

-- A top-K read with a `minmax` index on the ranked column reads a granule while its bound is within the running
-- boundary. Here the ranked column descends along the table, so every granule is read and the boundary tightens with
-- each of them. When the read ends, the granules whose rows all lie beyond the final boundary are recorded in the query
-- condition cache, so the repeated query selects far fewer granules than the first one.

-- CI randomizes these settings, and each of them can turn off the heap, its filter, the skip indexes on data read or the cache.
SET enable_group_by_top_k_optimization = 1;
SET enable_group_by_top_k_dynamic_filtering = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_for_top_k = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_top_k_dynamic_filtering = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_lazy_materialization = 0;
SET enable_parallel_replicas = 0;
-- The stateless-test profile sets `max_rows_to_read` and `max_rows_to_group_by`. A throwing row limit turns off the
-- skip indexes on data read, and a limit on the GROUP BY rows turns off the heap.
SET max_rows_to_read = 0, max_rows_to_read_leaf = 0;
SET max_rows_to_group_by = 0;
-- One stream reading one granule per block, so the boundary is set after the first granule.
SET max_threads = 1;
SET max_block_size = 128;

DROP TABLE IF EXISTS t_desc;
DROP TABLE IF EXISTS t_desc_unique;
DROP TABLE IF EXISTS t_desc_alter;
DROP TABLE IF EXISTS t_join_desc;
DROP TABLE IF EXISTS t_join_asc;
DROP TABLE IF EXISTS j_join;

CREATE TABLE t_desc (v UInt64, k UInt64, INDEX ik k TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY v
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_desc SELECT number, (75 - intDiv(number, 128)) * 100 + number % 37 FROM numbers(9600);
OPTIMIZE TABLE t_desc FINAL;

-- Unique values, so no row equals the boundary of `ORDER BY ... LIMIT` (rows equal to it may be dropped, and then the
-- cache is not written). The WHERE keeps the read from being narrowed to the granules of the smallest bounds up front.
CREATE TABLE t_desc_unique (v UInt64, k UInt64, s UInt8, INDEX ik k TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY v
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_desc_unique SELECT number, 9600 - number, number % 2 FROM numbers(9600);
OPTIMIZE TABLE t_desc_unique FINAL;

SELECT 'group by, first', arraySort(groupArray((k, c))) FROM (SELECT k, count() c FROM t_desc GROUP BY k LIMIT 7)
SETTINGS log_comment = '05233_qcc_groupby_1';
SELECT 'group by, repeated', arraySort(groupArray((k, c))) FROM (SELECT k, count() c FROM t_desc GROUP BY k LIMIT 7)
SETTINGS log_comment = '05233_qcc_groupby_2';
SELECT 'order by, first', arraySort(groupArray(k)) FROM (SELECT k FROM t_desc_unique WHERE s = 0 ORDER BY k LIMIT 7)
SETTINGS log_comment = '05233_qcc_orderby_1';
SELECT 'order by, repeated', arraySort(groupArray(k)) FROM (SELECT k FROM t_desc_unique WHERE s = 0 ORDER BY k LIMIT 7)
SETTINGS log_comment = '05233_qcc_orderby_2';

SYSTEM FLUSH LOGS query_log;

-- The first run selects all 75 granules; the repeated one hits the cache and selects only those it kept.
SELECT
    splitByChar('_', log_comment)[3] AS arm,
    anyIf(ProfileEvents['SelectedMarks'], endsWith(log_comment, '_1')) = 75,
    anyIf(ProfileEvents['QueryConditionCacheHits'], endsWith(log_comment, '_2')) > 0,
    anyIf(ProfileEvents['SelectedMarks'], endsWith(log_comment, '_2')) * 10 < anyIf(ProfileEvents['SelectedMarks'], endsWith(log_comment, '_1'))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment IN ('05233_qcc_groupby_1', '05233_qcc_groupby_2', '05233_qcc_orderby_1', '05233_qcc_orderby_2')
GROUP BY arm
ORDER BY arm
SETTINGS max_block_size = 65409;

-- A part that does not store a read column computes it from its DEFAULT on every read, so the values can differ on
-- the next run: the granules beyond the boundary are not recorded, and the repeated query selects all granules again.
CREATE TABLE t_desc_alter (v UInt64, k UInt64, INDEX ik k TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY v
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_desc_alter SELECT number, (75 - intDiv(number, 128)) * 100 + number % 37 FROM numbers(9600);
OPTIMIZE TABLE t_desc_alter FINAL;
ALTER TABLE t_desc_alter ADD COLUMN w UInt8 DEFAULT 1;

SELECT 'alter, first', arraySort(groupArray((k, c, sw))) FROM (SELECT k, count() c, sum(w) sw FROM t_desc_alter GROUP BY k LIMIT 7)
SETTINGS log_comment = '05233_qcc_alter_1';
SELECT 'alter, repeated', arraySort(groupArray((k, c, sw))) FROM (SELECT k, count() c, sum(w) sw FROM t_desc_alter GROUP BY k LIMIT 7)
SETTINGS log_comment = '05233_qcc_alter_2';

SYSTEM FLUSH LOGS query_log;

SELECT
    'alter',
    anyIf(ProfileEvents['SelectedMarks'], log_comment = '05233_qcc_alter_1') = 75,
    anyIf(ProfileEvents['SelectedMarks'], log_comment = '05233_qcc_alter_2') = anyIf(ProfileEvents['SelectedMarks'], log_comment = '05233_qcc_alter_1')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment IN ('05233_qcc_alter_1', '05233_qcc_alter_2')
SETTINGS max_block_size = 65409;

-- A grouping key computed by `joinGet` changes with the `Join` table, and with it the number of groups per value of the
-- ranked column, so the boundary of one run does not hold for the next: nothing is recorded, and after the `Join`
-- table changes the repeated query still returns seven groups of 128 rows, whether the ranked column descends or ascends.
CREATE TABLE j_join (b UInt64, g UInt64) ENGINE = Join(ANY, LEFT, b);
INSERT INTO j_join SELECT number, number FROM numbers(128);

CREATE TABLE t_join_desc (id UInt64, k UInt64, b UInt64, INDEX ik k TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_join_desc SELECT number, 75 - intDiv(number, 128), number % 128 FROM numbers(9600) SETTINGS max_block_size = 65409, max_insert_threads = 1;

CREATE TABLE t_join_asc (id UInt64, k UInt64, b UInt64, INDEX ik k TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_join_asc SELECT number, 1 + intDiv(number, 128), number % 128 FROM numbers(9600) SETTINGS max_block_size = 65409, max_insert_threads = 1;

SELECT 'join desc, first', count(), sum(c) FROM (SELECT k, joinGet('j_join', 'g', b) AS g, count() AS c FROM t_join_desc GROUP BY k, g LIMIT 7);
SELECT 'join asc, first', count(), sum(c) FROM (SELECT k, joinGet('j_join', 'g', b) AS g, count() AS c FROM t_join_asc GROUP BY k, g LIMIT 7);
TRUNCATE TABLE j_join;
INSERT INTO j_join SELECT number, 0 FROM numbers(128);
SELECT 'join desc, repeated', count(), sum(c) FROM (SELECT k, joinGet('j_join', 'g', b) AS g, count() AS c FROM t_join_desc GROUP BY k, g LIMIT 7);
SELECT 'join asc, repeated', count(), sum(c) FROM (SELECT k, joinGet('j_join', 'g', b) AS g, count() AS c FROM t_join_asc GROUP BY k, g LIMIT 7);

DROP TABLE t_desc;
DROP TABLE t_desc_unique;
DROP TABLE t_desc_alter;
DROP TABLE t_join_desc;
DROP TABLE t_join_asc;
DROP TABLE j_join;
