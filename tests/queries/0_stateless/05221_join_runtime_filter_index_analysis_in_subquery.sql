-- A join inside an `IN` subquery is executed on a clone of the subquery plan: `PreparedSets::build` runs
-- the speculative set build against `QueryPlan::clone` of the source plan so that the original stays
-- reusable. The join runtime filter descriptors registered on the probe-side `ReadFromMergeTree` have to
-- survive that clone (`ReadFromMergeTree::clone` copies them), otherwise the cloned build side keeps
-- tracking the key range while the cloned probe read can no longer prune a single granule with it.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET join_algorithm = 'hash';
SET max_bytes_ratio_before_external_join = 0;
SET use_skip_indexes_on_data_read = 1;
SET use_index_for_in_with_subqueries = 1;
SET transform_null_in = 0;

DROP TABLE IF EXISTS outer_in_pk;
DROP TABLE IF EXISTS probe_in_pk;
DROP TABLE IF EXISTS build_in_side;

-- The outer key is the primary key, so the set is built speculatively during index analysis of the outer
-- read, i.e. through the cloned subquery plan.
CREATE TABLE outer_in_pk (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO outer_in_pk SELECT number FROM numbers(1000);
CREATE TABLE probe_in_pk (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO probe_in_pk SELECT number, number FROM numbers(100000);
CREATE TABLE build_in_side (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO build_in_side SELECT number * 1000 FROM numbers(10);

SELECT count() FROM outer_in_pk WHERE k IN (SELECT p.v FROM probe_in_pk AS p INNER JOIN build_in_side AS b ON p.k = b.k)
    SETTINGS log_comment = '05221_in_subquery_join';

SYSTEM FLUSH LOGS query_log;
SELECT 'granules pruned inside the IN subquery';
SELECT
    argMax(ProfileEvents['RuntimeFiltersCreated'], event_time) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '05221_in_subquery_join'
    AND type = 'QueryFinish';

DROP TABLE outer_in_pk;
DROP TABLE probe_in_pk;
DROP TABLE build_in_side;
