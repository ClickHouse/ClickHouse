DROP VIEW IF EXISTS v_unusable_pk_limited;
DROP TABLE IF EXISTS t_unusable_pk;
DROP TABLE IF EXISTS t_usable_pk;

CREATE TABLE t_unusable_pk (s String, v UInt32) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
INSERT INTO t_unusable_pk SELECT toString(number * 11), number FROM numbers(6);

CREATE TABLE t_usable_pk (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_usable_pk SELECT number, number FROM numbers(100);

-- Every setting that gates building the throwaway probe plan is pinned here, because none of the
-- arms below can observe anything unless that plan is actually built: `parallel_replicas_local_plan`
-- is randomized by the test runner, and at 0 the probe is skipped and each arm would get its
-- expected error from the untouched single-node path instead.
SET enable_parallel_replicas = 1;
SET cluster_for_parallel_replicas = 'parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 1;
SET parallel_replicas_local_plan = 1;
SET max_parallel_replicas = 3;
SET parallel_replicas_mode = 'read_tasks';

-- A. The primary key cannot be used, so force_primary_key must reject the query. The replica-count
-- estimate runs first and must not report the row limit instead.
SET automatic_parallel_replicas_mode = 2;
SELECT v FROM t_unusable_pk WHERE startsWith(CAST(s, 'FixedString(40)'), '11') ORDER BY v
SETTINGS force_primary_key = 1, max_rows_to_read = 3; -- { serverError INDEX_NOT_USED }

-- B. The primary key is used and the read really does exceed max_rows_to_read, so the row limit is
-- still enforced.
SELECT v FROM t_usable_pk WHERE k >= 10 ORDER BY v
SETTINGS force_primary_key = 1, max_rows_to_read = 3; -- { serverError TOO_MANY_ROWS }

-- C. Same as A without parallel replicas.
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 0;
SELECT v FROM t_unusable_pk WHERE startsWith(CAST(s, 'FixedString(40)'), '11') ORDER BY v
SETTINGS force_primary_key = 1, max_rows_to_read = 3; -- { serverError INDEX_NOT_USED }

-- D. The row limit must still bite when parallel replicas really are used: mode 0 leaves
-- enable_parallel_replicas on the executed plan, which is the only case where the estimate replaces
-- an analysis the executed read would otherwise have reused.
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SELECT v FROM t_usable_pk WHERE k >= 10 ORDER BY v
SETTINGS force_primary_key = 1, max_rows_to_read = 3; -- { serverError TOO_MANY_ROWS }

-- E. Same as A through automatic_parallel_replicas_mode = 1.
-- automatic_parallel_replicas_min_bytes_per_replica = 0 switches the pre-planning size gate off, so
-- the probe plan is built for this tiny fixture.
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 1;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
SELECT v FROM t_unusable_pk WHERE startsWith(CAST(s, 'FixedString(40)'), '11') ORDER BY v
SETTINGS force_primary_key = 1, max_rows_to_read = 3; -- { serverError INDEX_NOT_USED }

-- F. Same as A through the arm that skips the query condition cache: with the TopK cache gate off, an
-- ORDER BY ... LIMIT read may still become a TopK read, so the estimate analyzes without the cache.
SET automatic_parallel_replicas_mode = 2;
SELECT v FROM t_unusable_pk WHERE startsWith(CAST(s, 'FixedString(40)'), '11') ORDER BY v LIMIT 1
SETTINGS force_primary_key = 1, max_rows_to_read = 3, use_query_condition_cache = 1,
    use_query_condition_cache_for_top_k = 0, use_skip_indexes_for_top_k = 1; -- { serverError INDEX_NOT_USED }

-- G. Same as A with the estimate analyzing a read found inside a view, where the row limit comes from
-- the view's own context rather than from the query selecting from it. Only a pass-through view is
-- read this way, so the inner query carries no ORDER BY.
SET parallel_replicas_allow_view_over_mergetree = 1;
CREATE VIEW v_unusable_pk_limited AS
    SELECT v FROM t_unusable_pk WHERE startsWith(CAST(s, 'FixedString(40)'), '11')
    SETTINGS max_rows_to_read = 3;
SELECT * FROM v_unusable_pk_limited SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

-- H. Same as A through the leaf row limit, the other clause of the gate.
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 2;
SELECT v FROM t_unusable_pk WHERE startsWith(CAST(s, 'FixedString(40)'), '11') ORDER BY v
SETTINGS force_primary_key = 1, max_rows_to_read_leaf = 3; -- { serverError INDEX_NOT_USED }

-- I. Same as B through the leaf row limit: it is still enforced when the read really exceeds it.
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 2;
SELECT v FROM t_usable_pk WHERE k >= 10 ORDER BY v
SETTINGS force_primary_key = 1, max_rows_to_read_leaf = 3; -- { serverError TOO_MANY_ROWS }

DROP VIEW v_unusable_pk_limited;
DROP TABLE t_unusable_pk;
DROP TABLE t_usable_pk;
