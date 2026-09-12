-- A `SQL SECURITY DEFINER` / `NONE` view that can hide rows is a barrier for the join runtime
-- filter index analysis too: `registerLeftSideIndexAnalysisSecondPass` used to walk from the
-- `__applyFilter` step (which correctly stays above the view's seal) down through the sealed
-- pass-through steps to the `ReadFromMergeTree` inside the view and register the invoker's
-- build-side keys for granule pruning there, making `read_rows` depend on the rows the view hides.

-- Pin everything the `read_rows` comparison depends on: the test also runs with randomized
-- settings. The runtime filter settings themselves are randomized by the harness; a single
-- thread, a stable join side and the read-path injections pinned off keep `read_rows` exactly
-- reproducible.
-- The runtime filter machinery only exists on the analyzer's logical join plan.
SET enable_analyzer = 1;

SET enable_join_runtime_filters = 1, enable_join_runtime_filters_index_analysis = 1,
    join_runtime_filter_min_probe_rows = 0, use_skip_indexes_on_data_read = 1, use_skip_indexes = 1,
    query_plan_join_swap_table = 'false', enable_parallel_replicas = 0, make_distributed_plan = 0,
    max_threads = 1,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    page_cache_inject_eviction = 0;

-- Twin tables, identical except for the primary-key value of the single hidden row: past the end
-- of the last visible granule in one, far past it in the other. The build side probes the key
-- between the two, so index analysis driven from outside the view prunes the last granule in one
-- twin and keeps it in the other, and the two reads diverge.
DROP TABLE IF EXISTS t04892_a;
DROP TABLE IF EXISTS t04892_b;
DROP TABLE IF EXISTS t04892_build;
CREATE TABLE t04892_a (k UInt64, owner String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024;
CREATE TABLE t04892_b (k UInt64, owner String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024;
INSERT INTO t04892_a SELECT number, 'nobody' FROM numbers(100000);
INSERT INTO t04892_a VALUES (200000, 'hidden');
INSERT INTO t04892_b SELECT number, 'nobody' FROM numbers(100000);
INSERT INTO t04892_b VALUES (300000, 'hidden');
OPTIMIZE TABLE t04892_a FINAL;
OPTIMIZE TABLE t04892_b FINAL;

CREATE TABLE t04892_build (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04892_build VALUES (250000);

CREATE VIEW v04892_a DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k FROM t04892_a WHERE owner != 'hidden';
CREATE VIEW v04892_b DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k FROM t04892_b WHERE owner != 'hidden';
CREATE VIEW v04892_inv SQL SECURITY INVOKER AS SELECT k FROM t04892_a WHERE owner != 'hidden';

SELECT count() FROM v04892_a INNER JOIN t04892_build ON v04892_a.k = t04892_build.k SETTINGS log_comment = '04892_probe_definer_a';
SELECT count() FROM v04892_b INNER JOIN t04892_build ON v04892_b.k = t04892_build.k SETTINGS log_comment = '04892_probe_definer_b';
-- The `INVOKER` view stays fully optimizable: the build-side key prunes almost everything. This
-- is the positive control proving that the index analysis fires and that the oracle can see it.
SELECT count() FROM v04892_inv INNER JOIN t04892_build ON v04892_inv.k = t04892_build.k SETTINGS log_comment = '04892_probe_invoker_a';

SYSTEM FLUSH LOGS query_log;
-- `count() != 3` guards against the comparisons passing vacuously on an empty match.
SELECT 'reading the view costs the same whatever the hidden key is:', multiIf(
        count() != 3, 'MISSING',
        anyIf(read_rows, log_comment = '04892_probe_definer_a') != anyIf(read_rows, log_comment = '04892_probe_definer_b'), 'DISCLOSED',
        anyIf(read_rows, log_comment = '04892_probe_invoker_a') >= anyIf(read_rows, log_comment = '04892_probe_definer_a'), 'CONTROL DID NOT PRUNE',
        'same, and the invoker control pruned')
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment LIKE '04892_probe_%' AND type = 'QueryFinish';

DROP VIEW v04892_a;
DROP VIEW v04892_b;
DROP VIEW v04892_inv;
DROP TABLE t04892_a;
DROP TABLE t04892_b;
DROP TABLE t04892_build;
