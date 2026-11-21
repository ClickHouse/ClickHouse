-- The query condition cache must not reuse a verdict across an `ALTER` that changes the default or the `MATERIALIZED` expression of a column the parts do not store.
-- Every case prints the count with the cache and without it.

SET use_query_condition_cache = 1;
-- Without a local plan the filter steps run as part of the remote queries, and this server's cache sees nothing.
SET parallel_replicas_local_plan = 1;
-- The pending mutations below must not run, and the `ALTER`s must not wait for them.
SET mutations_sync = 0;
SET alter_sync = 0;
SET session_timezone = 'UTC';
-- WHERE cases must stay WHERE: the automatic move to PREWHERE would exercise the other writer.
SET optimize_move_to_prewhere = 0;

-- The auto minmax indexes would answer before the cache, and the cache stores nothing for small parts.
-- MODIFY COLUMN ... DEFAULT of a column the parts do not store, WHERE and PREWHERE.
CREATE TABLE t_default (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_default;
INSERT INTO t_default SELECT number FROM numbers(100000);
ALTER TABLE t_default ADD COLUMN y UInt8 DEFAULT 7;
SELECT count() FROM t_default WHERE y = 0 FORMAT Null;
ALTER TABLE t_default MODIFY COLUMN y UInt8 DEFAULT 0;
SELECT 'MODIFY DEFAULT, WHERE', count() FROM t_default WHERE y = 0;
SELECT 'MODIFY DEFAULT, WHERE', count() FROM t_default WHERE y = 0 SETTINGS use_query_condition_cache = 0;

CREATE TABLE t_default_prewhere (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_default_prewhere;
INSERT INTO t_default_prewhere SELECT number FROM numbers(100000);
ALTER TABLE t_default_prewhere ADD COLUMN y UInt8 DEFAULT 7;
SELECT count() FROM t_default_prewhere PREWHERE y = 0 FORMAT Null;
ALTER TABLE t_default_prewhere MODIFY COLUMN y UInt8 DEFAULT 0;
SELECT 'MODIFY DEFAULT, PREWHERE', count() FROM t_default_prewhere PREWHERE y = 0;
SELECT 'MODIFY DEFAULT, PREWHERE', count() FROM t_default_prewhere PREWHERE y = 0 SETTINGS use_query_condition_cache = 0;

-- A repeated condition still reads the verdict recorded after the ALTER.
SELECT count() FROM t_default WHERE y = 7 FORMAT Null;
SELECT count() FROM t_default WHERE y = 7 SETTINGS log_comment = '05325_where_hit' FORMAT Null;
SELECT count() FROM t_default_prewhere PREWHERE y = 7 FORMAT Null;
SELECT count() FROM t_default_prewhere PREWHERE y = 7 SETTINGS log_comment = '05325_prewhere_hit' FORMAT Null;

-- MODIFY COLUMN ... REMOVE DEFAULT.
CREATE TABLE t_remove_default (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_remove_default;
INSERT INTO t_remove_default SELECT number FROM numbers(100000);
ALTER TABLE t_remove_default ADD COLUMN y UInt8 DEFAULT 7;
SELECT count() FROM t_remove_default WHERE y = 0 FORMAT Null;
ALTER TABLE t_remove_default MODIFY COLUMN y REMOVE DEFAULT;
SELECT 'REMOVE DEFAULT', count() FROM t_remove_default WHERE y = 0;
SELECT 'REMOVE DEFAULT', count() FROM t_remove_default WHERE y = 0 SETTINGS use_query_condition_cache = 0;

-- MODIFY COLUMN ... MATERIALIZED.
CREATE TABLE t_materialized (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_materialized;
INSERT INTO t_materialized SELECT number FROM numbers(100000);
ALTER TABLE t_materialized ADD COLUMN y UInt8 MATERIALIZED 7;
SELECT count() FROM t_materialized WHERE y = 0 FORMAT Null;
ALTER TABLE t_materialized MODIFY COLUMN y UInt8 MATERIALIZED 0;
SELECT 'MODIFY MATERIALIZED', count() FROM t_materialized WHERE y = 0;
SELECT 'MODIFY MATERIALIZED', count() FROM t_materialized WHERE y = 0 SETTINGS use_query_condition_cache = 0;

-- A default that depends on another column.
CREATE TABLE t_expression (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_expression;
INSERT INTO t_expression SELECT number FROM numbers(100000);
ALTER TABLE t_expression ADD COLUMN y UInt64 DEFAULT k;
SELECT count() FROM t_expression WHERE y = k + 1 FORMAT Null;
ALTER TABLE t_expression MODIFY COLUMN y UInt64 DEFAULT k + 1;
SELECT 'MODIFY DEFAULT expression', count() FROM t_expression WHERE y = k + 1;
SELECT 'MODIFY DEFAULT expression', count() FROM t_expression WHERE y = k + 1 SETTINGS use_query_condition_cache = 0;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['QueryConditionCacheHits'] > 0, ProfileEvents['SelectedMarks'] < ProfileEvents['SelectedMarksTotal']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05325_where_hit', '05325_prewhere_hit')
ORDER BY log_comment;
