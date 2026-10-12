-- Tags: no-parallel, no-fasttest
-- no-parallel: Uses the `prepared_sets_build_ordered_set_inplace_fail` failpoint, which is global.
-- no-fasttest: tests that run alone (because of the failpoint) are kept out of the fast test.
--
-- Regression test: an `IN` subquery that reads a materialized CTE must still produce the correct
-- result when its speculative in-place set build for primary key analysis stops without creating
-- the set. The failpoint skips `Set::finishInsert` once, so the set is left not created on the
-- in-place pass, and the deferred build must create it from the preserved subquery plan.
--
-- A plain `WITH A AS (...)` is inlined at each reference and collapses to the ordinary `IN` shape
-- covered by `04489_not_ready_set_inplace_build`, so the CTE is `MATERIALIZED` here: it survives
-- planning as a real CTE table node. The `enabled` column of `system.fail_points` flipping from `1`
-- to `0` proves that the in-place build was really faulted, so the test cannot pass vacuously.

DROP TABLE IF EXISTS 04094_data;
DROP TABLE IF EXISTS 04094_keys;

CREATE TABLE 04094_data
(
    key String,
    value UInt8
)
ENGINE = MergeTree
ORDER BY key;

CREATE TABLE 04094_keys
(
    key String
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO 04094_data VALUES ('a', 1), ('b', 2), ('c', 3);
INSERT INTO 04094_keys VALUES ('a'), ('x');

SET use_index_for_in_with_subqueries = 1;
SET enable_materialized_cte = 1;

SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
SELECT enabled FROM system.fail_points WHERE name = 'prepared_sets_build_ordered_set_inplace_fail';
WITH A AS MATERIALIZED (SELECT key FROM 04094_keys)
SELECT count() == 1
FROM 04094_data
WHERE key IN (SELECT key FROM A);
SELECT enabled FROM system.fail_points WHERE name = 'prepared_sets_build_ordered_set_inplace_fail';
SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

DROP TABLE 04094_keys;
DROP TABLE 04094_data;
