-- The query condition cache must not reuse a verdict across an `ALTER` that changes what parts return for a column,
-- nor record one computed from a non-deterministic default. Every case prints the count with the cache and without it.

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

-- A pending type change of a column the default depends on, with and without mutations applied on the fly.
CREATE TABLE t_type (k UInt64, d Date) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_type;
INSERT INTO t_type SELECT number, '2020-01-01' FROM numbers(100000);
ALTER TABLE t_type ADD COLUMN s String DEFAULT toString(d);
SELECT count() FROM t_type WHERE s = '2020-01-01 00:00:00' FORMAT Null;
ALTER TABLE t_type MODIFY COLUMN d DateTime;
SELECT 'MODIFY type', count() FROM t_type WHERE s = '2020-01-01 00:00:00';
SELECT 'MODIFY type', count() FROM t_type WHERE s = '2020-01-01 00:00:00' SETTINGS use_query_condition_cache = 0;

CREATE TABLE t_type_not_on_fly (k UInt64, d Date) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_type_not_on_fly;
INSERT INTO t_type_not_on_fly SELECT number, '2020-01-01' FROM numbers(100000);
ALTER TABLE t_type_not_on_fly ADD COLUMN s String DEFAULT toString(d);
SELECT count() FROM t_type_not_on_fly WHERE s = '2020-01-01 00:00:00' SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0 FORMAT Null;
ALTER TABLE t_type_not_on_fly MODIFY COLUMN d DateTime;
SELECT 'MODIFY type, not on the fly', count() FROM t_type_not_on_fly WHERE s = '2020-01-01 00:00:00' SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0;
SELECT 'MODIFY type, not on the fly', count() FROM t_type_not_on_fly WHERE s = '2020-01-01 00:00:00' SETTINGS use_query_condition_cache = 0, apply_mutations_on_fly = 0, apply_patch_parts = 0;

-- A time zone change of a column the default depends on, which needs no mutation.
CREATE TABLE t_timezone (k UInt64, d DateTime('UTC')) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_timezone;
INSERT INTO t_timezone SELECT number, '2020-01-01 00:00:00' FROM numbers(100000);
ALTER TABLE t_timezone ADD COLUMN s String DEFAULT toString(d);
SELECT count() FROM t_timezone WHERE s = '2020-01-01 09:00:00' FORMAT Null;
ALTER TABLE t_timezone MODIFY COLUMN d DateTime('Asia/Tokyo');
SELECT 'MODIFY time zone', count() FROM t_timezone WHERE s = '2020-01-01 09:00:00';
SELECT 'MODIFY time zone', count() FROM t_timezone WHERE s = '2020-01-01 09:00:00' SETTINGS use_query_condition_cache = 0;

-- DROP COLUMN and ADD COLUMN of the same column while the drop is pending, then KILL MUTATION.
-- Part pruning by statistics and the trivial count over them do not see the pending drop, so they are off here.
CREATE TABLE t_drop_add (k UInt64, y UInt8) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_drop_add;
INSERT INTO t_drop_add SELECT number, 1 FROM numbers(100000);
SELECT count() FROM t_drop_add WHERE y = 0 SETTINGS use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0 FORMAT Null;
ALTER TABLE t_drop_add DROP COLUMN y;
ALTER TABLE t_drop_add ADD COLUMN y UInt8;
SELECT 'DROP and ADD', count() FROM t_drop_add WHERE y = 0 SETTINGS use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0;
SELECT 'DROP and ADD', count() FROM t_drop_add WHERE y = 0 SETTINGS use_query_condition_cache = 0, use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0;

CREATE TABLE t_drop_add_kill (k UInt64, y UInt8) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_drop_add_kill;
INSERT INTO t_drop_add_kill SELECT number, 1 FROM numbers(100000);
ALTER TABLE t_drop_add_kill DROP COLUMN y;
ALTER TABLE t_drop_add_kill ADD COLUMN y UInt8;
SELECT count() FROM t_drop_add_kill WHERE y = 1 SETTINGS use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0 FORMAT Null;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_drop_add_kill' SYNC FORMAT Null;
SELECT 'DROP and ADD, KILL MUTATION', count() FROM t_drop_add_kill WHERE y = 1 SETTINGS use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0;
SELECT 'DROP and ADD, KILL MUTATION', count() FROM t_drop_add_kill WHERE y = 1 SETTINGS use_query_condition_cache = 0, use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0;

-- DROP COLUMN and ADD COLUMN of a column a `SELECT *` projection stores, while the drop is pending.
CREATE TABLE t_drop_add_projection (k UInt64, k2 UInt64, y UInt8, PROJECTION p (SELECT * ORDER BY k2)) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_drop_add_projection;
INSERT INTO t_drop_add_projection SELECT number, number, 1 FROM numbers(100000);
SELECT count() FROM t_drop_add_projection WHERE y = 0
    SETTINGS optimize_use_projections = 1, prefer_optimize_projection = 1, optimize_use_implicit_projections = 0, enable_parallel_replicas = 0,
    use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0 FORMAT Null;
ALTER TABLE t_drop_add_projection DROP COLUMN y;
ALTER TABLE t_drop_add_projection ADD COLUMN y UInt8;
SELECT 'DROP and ADD, projection', count() FROM t_drop_add_projection WHERE y = 0
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 0, enable_parallel_replicas = 0,
    use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0;
SELECT 'DROP and ADD, projection', count() FROM t_drop_add_projection WHERE y = 0
    SETTINGS use_query_condition_cache = 0, optimize_use_projections = 1, optimize_use_implicit_projections = 0, enable_parallel_replicas = 0,
    use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0;

-- Non-deterministic defaults: `thr` is 5 seconds after each table's INSERT and the warm-up runs right after it, so no row matches before the sleep and every row after it.
CREATE TABLE t_now (k UInt64, v UInt64, thr DateTime64(3)) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now;
INSERT INTO t_now SELECT number, number, now64(3) + 5 FROM numbers(100000);
ALTER TABLE t_now ADD COLUMN ts DateTime64(3) DEFAULT now64(3);
SELECT count() FROM t_now WHERE ts >= thr FORMAT Null;

CREATE TABLE t_now_prewhere (k UInt64, v UInt64, thr DateTime64(3)) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now_prewhere;
INSERT INTO t_now_prewhere SELECT number, number, now64(3) + 5 FROM numbers(100000);
ALTER TABLE t_now_prewhere ADD COLUMN ts DateTime64(3) DEFAULT now64(3);
SELECT count() FROM t_now_prewhere PREWHERE ts >= thr FORMAT Null;

-- The same default in a tuple, filtered by a subcolumn.
CREATE TABLE t_now_tuple (k UInt64, thr DateTime64(3)) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now_tuple;
INSERT INTO t_now_tuple SELECT number, now64(3) + 5 FROM numbers(100000);
ALTER TABLE t_now_tuple ADD COLUMN tup Tuple(a DateTime64(3), b UInt8) DEFAULT (now64(3), 1);
SELECT count() FROM t_now_tuple WHERE tup.a >= thr FORMAT Null;

-- A non-deterministic default that replaces NULLs while a change to a non-Nullable type is pending.
CREATE TABLE t_now_nullable (k UInt64, v Nullable(DateTime64(3)), thr DateTime64(3)) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now_nullable;
INSERT INTO t_now_nullable SELECT number, NULL, now64(3) + 5 FROM numbers(100000);
ALTER TABLE t_now_nullable MODIFY COLUMN v DateTime64(3) DEFAULT now64(3);
SELECT count() FROM t_now_nullable WHERE v >= thr FORMAT Null;

CREATE TABLE t_now_nullable_prewhere (k UInt64, v Nullable(DateTime64(3)), thr DateTime64(3)) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now_nullable_prewhere;
INSERT INTO t_now_nullable_prewhere SELECT number, NULL, now64(3) + 5 FROM numbers(100000);
ALTER TABLE t_now_nullable_prewhere MODIFY COLUMN v DateTime64(3) DEFAULT now64(3);
SELECT count() FROM t_now_nullable_prewhere PREWHERE v >= thr FORMAT Null;

-- A default that reads such a column.
CREATE TABLE t_now_nullable_dependent (k UInt64, v Nullable(DateTime64(3)), thr DateTime64(3)) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now_nullable_dependent;
INSERT INTO t_now_nullable_dependent SELECT number, NULL, now64(3) + 5 FROM numbers(100000);
ALTER TABLE t_now_nullable_dependent MODIFY COLUMN v DateTime64(3) DEFAULT now64(3);
ALTER TABLE t_now_nullable_dependent ADD COLUMN w DateTime64(3) DEFAULT v;
SELECT count() FROM t_now_nullable_dependent WHERE w >= thr FORMAT Null;

-- ORDER BY ... LIMIT: the part without `v` sets the threshold the granules of the other part are dropped by.
CREATE TABLE t_now_top_k (k UInt64, tag UInt8, keep UInt8) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now_top_k;
INSERT INTO t_now_top_k SELECT number, 0, 1 FROM numbers(100000);
ALTER TABLE t_now_top_k ADD COLUMN v DateTime64(3) DEFAULT now64(3) + toIntervalMillisecond(k);
INSERT INTO t_now_top_k SELECT number, 1, 1, now64(3) + 5 FROM numbers(100000);
SELECT tag FROM t_now_top_k WHERE keep = 1 ORDER BY v LIMIT 1
    SETTINGS use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1
    FORMAT Null;

SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;
SELECT 'DEFAULT now64, WHERE', count() FROM t_now WHERE ts >= thr;
SELECT 'DEFAULT now64, WHERE', count() FROM t_now WHERE ts >= thr SETTINGS use_query_condition_cache = 0;
SELECT 'DEFAULT now64, PREWHERE', count() FROM t_now_prewhere PREWHERE ts >= thr;
SELECT 'DEFAULT now64, PREWHERE', count() FROM t_now_prewhere PREWHERE ts >= thr SETTINGS use_query_condition_cache = 0;
SELECT 'DEFAULT now64, subcolumn', count() FROM t_now_tuple WHERE tup.a >= thr;
SELECT 'DEFAULT now64, subcolumn', count() FROM t_now_tuple WHERE tup.a >= thr SETTINGS use_query_condition_cache = 0;
SELECT 'DEFAULT now64 for NULL, WHERE', count() FROM t_now_nullable WHERE v >= thr;
SELECT 'DEFAULT now64 for NULL, WHERE', count() FROM t_now_nullable WHERE v >= thr SETTINGS use_query_condition_cache = 0;
SELECT 'DEFAULT now64 for NULL, PREWHERE', count() FROM t_now_nullable_prewhere PREWHERE v >= thr;
SELECT 'DEFAULT now64 for NULL, PREWHERE', count() FROM t_now_nullable_prewhere PREWHERE v >= thr SETTINGS use_query_condition_cache = 0;
SELECT 'DEFAULT reading a column filled by now64', count() FROM t_now_nullable_dependent WHERE w >= thr;
SELECT 'DEFAULT reading a column filled by now64', count() FROM t_now_nullable_dependent WHERE w >= thr SETTINGS use_query_condition_cache = 0;
SELECT 'DEFAULT now64, ORDER BY LIMIT', tag FROM t_now_top_k WHERE keep = 1 ORDER BY v LIMIT 1
    SETTINGS use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1;
SELECT 'DEFAULT now64, ORDER BY LIMIT', tag FROM t_now_top_k WHERE keep = 1 ORDER BY v LIMIT 1
    SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1;

-- A condition on stored columns is still cached when the query also reads such a default.
-- The WHERE writer records whole blocks, so a block must not span the matching and the non-matching granules.
SELECT ts FROM t_now WHERE v < 10000 SETTINGS max_block_size = 8192 FORMAT Null;
SELECT ts FROM t_now WHERE v < 10000 SETTINGS max_block_size = 8192, log_comment = '05325_now_where_hit' FORMAT Null;
SELECT ts FROM t_now_prewhere PREWHERE v < 10000 FORMAT Null;
SELECT ts FROM t_now_prewhere PREWHERE v < 10000 SETTINGS log_comment = '05325_now_prewhere_hit' FORMAT Null;
-- ORDER BY ... LIMIT is still cached when every part stores the column with the non-deterministic default.
CREATE TABLE t_now_top_k_stored (k UInt64, tag UInt8, keep UInt8, v DateTime64(3) DEFAULT now64(3) + toIntervalMillisecond(k)) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now_top_k_stored;
INSERT INTO t_now_top_k_stored (k, tag, keep) SELECT number, 0, 1 FROM numbers(100000);
INSERT INTO t_now_top_k_stored SELECT number, 1, 1, now64(3) + 5 FROM numbers(100000);
SELECT tag FROM t_now_top_k_stored WHERE keep = 1 ORDER BY v LIMIT 1
    SETTINGS use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1
    FORMAT Null;
SELECT tag FROM t_now_top_k_stored WHERE keep = 1 ORDER BY v LIMIT 1
    SETTINGS use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1,
    log_comment = '05325_top_k_hit' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['QueryConditionCacheHits'] > 0, ProfileEvents['SelectedMarks'] < ProfileEvents['SelectedMarksTotal']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05325_where_hit', '05325_prewhere_hit', '05325_now_where_hit', '05325_now_prewhere_hit', '05325_top_k_hit')
ORDER BY log_comment;
