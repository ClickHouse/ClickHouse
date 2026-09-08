-- Tags: no-parallel
-- no-parallel: uses the global ONCE failpoint `prepared_sets_build_ordered_set_inplace_fail`.

-- `prepared_sets_build_ordered_set_inplace_fail` simulates a speculative in-place set build that
-- stops without creating the set, which the runtime build then redoes. A runtime build has no such
-- second chance: abandoning one leaves the set not created, and `FunctionIn` reports "Not-ready Set
-- is passed as the second argument" from inside the running `FilterTransform`, aborting a debug
-- server. So the injection must reach a speculative build only, and must not consume its one shot on
-- a runtime build either, or the speculative build it was armed for runs unfaulted.

DROP TABLE IF EXISTS t_fp_scope;
CREATE TABLE t_fp_scope (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_fp_scope SELECT number FROM numbers(1000);
SET use_index_for_in_with_subqueries = 1;

SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
SELECT enabled FROM system.fail_points WHERE name = 'prepared_sets_build_ordered_set_inplace_fail';

-- `system.one` has no primary key and no skip index, so no in-place build runs and this set is built
-- only at runtime. It must be neither abandoned nor charged the one shot.
SELECT 1 WHERE 1 IN (SELECT 1);
SELECT enabled FROM system.fail_points WHERE name = 'prepared_sets_build_ordered_set_inplace_fail';

-- `k` is the primary key, so this `IN` does run the in-place build: the shot lands there and the
-- runtime build recovers the set, so the count is still correct and the failpoint is now spent.
SELECT count() FROM t_fp_scope WHERE k IN (SELECT k FROM t_fp_scope WHERE k < 500);
SELECT enabled FROM system.fail_points WHERE name = 'prepared_sets_build_ordered_set_inplace_fail';

SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
DROP TABLE t_fp_scope;
