-- Tags: no-parallel
-- no-parallel: uses the global ONCE failpoint `prepared_sets_build_ordered_set_inplace_fail`.

-- `prepared_sets_build_ordered_set_inplace_fail` simulates an in-place set build that stops without
-- creating the set. Only one build can survive that: the in-place build that runs against a clone of
-- the subquery source, whose original `source` the deferred build then redoes. Every other build
-- consumes `source` (the runtime build, and the two in-place builds that go through
-- `FutureSetFromSubquery::build`), so abandoning one leaves the set uncreated for good, `FunctionIn`
-- reports "Not-ready Set is passed as the second argument" from inside the running `FilterTransform`,
-- and a debug server aborts. So the injection must reach the clone-backed build only, and must not
-- spend its one shot anywhere else, or the build it was armed for runs unfaulted. Those two
-- unfaultable in-place builds are `buildSetInplace` and `buildOrderedSetInplace`'s fallback for a
-- source that cannot be cloned, and there is an arm below for each.

DROP TABLE IF EXISTS t_fp_scope;
CREATE TABLE t_fp_scope (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_fp_scope SELECT number FROM numbers(1000);
SET use_index_for_in_with_subqueries = 1;

-- Pins the premise of the runtime arm below, so it cannot go vacuous if `1 IN (SELECT 1)` is ever
-- folded into a scalar comparison instead of being built as a set.
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT 1 WHERE 1 IN (SELECT 1))
WHERE explain ILIKE '%CreatingSetsTransform%';

SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
SELECT enabled FROM system.fail_points WHERE name = 'prepared_sets_build_ordered_set_inplace_fail';

-- `system.one` has no primary key and no skip index, so no in-place build runs and this set is built
-- only at runtime. It must be neither abandoned nor charged the one shot.
SELECT 1 WHERE 1 IN (SELECT 1);
SELECT enabled FROM system.fail_points WHERE name = 'prepared_sets_build_ordered_set_inplace_fail';

-- `system.tables` filtering builds its set through the destructive
-- `VirtualColumnUtils::buildSetsForDAG` -> `FutureSetFromSubquery::buildSetInplace` path, which
-- consumes the subquery source, so abandoning it would leave the set uncreated for good: the shot
-- must not land here, and the filter must still be applied.
SELECT count() > 0 AND min(database = 'system')
FROM system.tables WHERE database IN (SELECT 'system');
SELECT enabled FROM system.fail_points WHERE name = 'prepared_sets_build_ordered_set_inplace_fail';

-- The ordered in-place build falls back to the destructive path when the subquery source cannot be
-- cloned: `system.build_options` reads through a step that does not implement `clone()`, so
-- `buildOrderedSetInplace` catches NOT_IMPLEMENTED and builds through `FutureSetFromSubquery::build`,
-- which moves `source` out. Abandoning that build would leave the set uncreated for good, so the shot
-- must not land here either. The error-counter delta around the query is what proves the clone was
-- really rejected, so this arm cannot pass by quietly taking the clone-backed path instead.
CREATE TABLE t_fp_scope_errors (value UInt64) ENGINE = MergeTree ORDER BY value;
INSERT INTO t_fp_scope_errors SELECT sum(value) FROM system.errors WHERE name = 'NOT_IMPLEMENTED';
SELECT count() FROM t_fp_scope WHERE k IN (SELECT toUInt64(1) FROM system.build_options LIMIT 1);
INSERT INTO t_fp_scope_errors SELECT sum(value) FROM system.errors WHERE name = 'NOT_IMPLEMENTED';
SELECT max(value) > min(value) FROM t_fp_scope_errors;
SELECT enabled FROM system.fail_points WHERE name = 'prepared_sets_build_ordered_set_inplace_fail';
DROP TABLE t_fp_scope_errors;

-- `k` is the primary key, so this `IN` does run the in-place build, and a plain `MergeTree` read is
-- clonable: the shot lands on the clone, the deferred build still creates the set from the preserved
-- `source`, so the count is correct and the failpoint is now spent.
SELECT count() FROM t_fp_scope WHERE k IN (SELECT k FROM t_fp_scope WHERE k < 500);
SELECT enabled FROM system.fail_points WHERE name = 'prepared_sets_build_ordered_set_inplace_fail';

SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
DROP TABLE t_fp_scope;
